"""Deepstream RPC handling."""

from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy.constants import topic as topic_constants
from deepstreampy.constants import actions
from deepstreampy.constants import event as event_constants
from deepstreampy.message import message_builder
from deepstreampy.message import message_parser
from deepstreampy import utils

from tornado import concurrent

from functools import partial


class RPCResponse(object):
    """Allows a RPC provider to respond to a request.

    Attributes:
        auto_ack (bool): Specifies whether requests should be auto acknowledged
    """

    def __init__(self, connection, name, correlation_id):
        """
        Args:
            connection (deepstreampy.client._Connection): The current connection
            name (str): The name of the RPC
            correlation_id (str): Correlation ID of the RPC
        """
        self._connection = connection
        self._name = name
        self._correletaion_id = correlation_id
        self._is_acknowledged = False
        self._is_complete = False
        self.auto_ack = True

        self._connection._io_loop.add_callback(self._perform_auto_ack)

    def ack(self):
        """Acknowledge the receiving the request.

        Will happen implicitly unless the request callback sets ``auto_ack``
        to False.

        """
        if not self._is_acknowledged:
            future = self._connection.send_message(
                topic_constants.RPC,
                actions.ACK,
                [actions.REQUEST, self._name, self._correletaion_id])
            self._is_acknowledged = True
        else:
            future = concurrent.Future()
            future.set_result(None)

        return future

    def reject(self):
        """Reject the request.

        This might be necessary if the client is already processing a large
        number of requests. If deepstream receives a rejection message it will
        try to route the request to another provider - or return a
        NO_RPC_PROVIDER error if there are no providers left.

        If autoAck is disabled and the response is sent before the ack message
        the request will still be completed and the ack message ignored.

        """
        self.auto_ack = False
        self._is_complete = True
        self._is_acknowledged = True
        return self._connection.send_message(
            topic_constants.RPC,
            actions.REJECTION,
            [self._name, self._correletaion_id])

    def send(self, data):
        """Complete the request by sending the response data to the server.

        Args:
            data: JSON serializable data to send to the server.
        """
        if self._is_complete:
            raise ValueError('RPC {0} already completed'.format(self._name))
        self.ack()

        typed_data = message_builder.typed(data)
        self._is_complete = True

        return self._connection.send_message(
            topic_constants.RPC,
            actions.RESPONSE,
            [self._name, self._correletaion_id, typed_data])

    def error(self, error_str):
        """Notify the server that an error has occured.

        This will also complete the RPC.
        """
        self.auto_ack = False
        self._is_complete = True
        self._is_acknowledged = True
        return self._connection.send_message(
            topic_constants.RPC,
            actions.ERROR,
            [error_str, self._name, self._correletaion_id])

    def _perform_auto_ack(self):
        if self.auto_ack:
            self.ack()


class RPC(object):
    """Represents a single RPC made from the client to the server.

    Encapsulates logic around timeouts and converts the incoming response data.
    """

    def __init__(self, callback, client, **options):
        self._options = options
        self._callback = callback
        self._client = client
        self._connection = client._connection

        self._ack_timeout = self._connection._io_loop.call_later(
            options.get('rpcAckTimeout', 6),
            partial(self.error, event_constants.ACK_TIMEOUT))

        self._response_timeout = self._connection._io_loop.call_later(
            options.get('rpcResponseTimeout', 6),
            partial(self.error, event_constants.RESPONSE_TIMEOUT))

    def ack(self):
        self._connection._io_loop.remove_timeout(self._ack_timeout)

    def respond(self, data):
        converted_data = message_parser.convert_typed(data, self._client)
        self._callback(None, converted_data)
        self._complete()

    def error(self, error_msg):
        self._callback(error_msg, None)
        self._complete()

    def _complete(self):
        self._connection._io_loop.remove_timeout(self._ack_timeout)
        self._connection._io_loop.remove_timeout(self._response_timeout)


class RPCHandler(object):

    def __init__(self, connection, client, **options):
        self._options = options
        self._connection = connection
        self._client = client
        self._rpcs = {}
        self._providers = {}
        self._provide_ack_timeouts = {}

        subscription_timeout = options.get("subscriptionTimeout", 15)
        self._ack_timeout_registry = utils.AckTimeoutRegistry(
            client, topic_constants.RPC, subscription_timeout)
        self._resubscribe_notifier = utils.ResubscribeNotifier(
            client, self._reprovide)

    def provide(self, name, callback):
        if not name:
            raise ValueError("invalid argument name")
        if name in self._providers:
            raise ValueError("RPC {0} already registered".format(name))

        self._ack_timeout_registry.add(name, actions.SUBSCRIBE)
        self._providers[name] = callback

        return self._connection.send_message(topic_constants.RPC,
                                             actions.SUBSCRIBE,
                                             [name])

    def unprovide(self, name):
        if not name:
            raise ValueError("invalid argument name")

        if name in self._providers:
            del self._providers[name]
            self._ack_timeout_registry.add(name, actions.UNSUBSCRIBE)
            future = self._connection.send_message(topic_constants.RPC,
                                                   actions.UNSUBSCRIBE,
                                                   [name])
        else:
            future = concurrent.Future()
            future.set_result(None)

        return future

    def make(self, name, data, callback):
        if not name:
            raise ValueError("invalid argument name")

        uid = utils.get_uid()
        typed_data = message_builder.typed(data)
        self._rpcs[uid] = RPC(callback, self._client, **self._options)

        return self._connection.send_message(
            topic_constants.RPC, actions.REQUEST, [name, uid, typed_data])

    def _get_rpc(self, correlation_id, rpc_name, raw_message):
        if correlation_id not in self._rpcs:
            self._client._on_error(topic_constants.RPC,
                                   event_constants.UNSOLICITED_MESSAGE,
                                   raw_message)
            return

        rpc = self._rpcs[correlation_id]
        return rpc

    def _respond_to_rpc(self, message):
        name = message['data'][0]
        correlation_id = message['data'][1]

        if message['data'][2]:
            data = message_parser.convert_typed(message['data'][2],
                                                self._client)

        if name in self._providers:
            response = RPCResponse(self._connection, name, correlation_id)
            self._providers[name](data, response)
        else:
            self._connection.send_message(topic_constants.RPC,
                                          actions.REJECTION,
                                          [name, correlation_id])

    def handle(self, message):
        action = message['action']
        data = message['data']
        if action == actions.REQUEST:
            self._respond_to_rpc(message)
            return

        if (action == actions.ACK and
                (data[0] in (actions.SUBSCRIBE, actions.UNSUBSCRIBE))):
            self._ack_timeout_registry.clear(message)
            return

        if action == actions.ERROR:
            if data[0] == event_constants.MESSAGE_PERMISSION_ERROR:
                return

            if (data[0] == event_constants.MESSAGE_DENIED and
                    data[2] == actions.SUBSCRIBE):
                self._ack_timeout_registry.remove(data[1], actions.SUBSCRIBE)
                return

        if action in (actions.ERROR, actions.ACK):
            if (data[0] == event_constants.MESSAGE_DENIED and
                    data[2] == actions.REQUEST):
                correlation_id = data[3]
            else:
                correlation_id = data[2]
            rpc_name = data[1]
        else:
            rpc_name = data[0]
            correlation_id = data[1]

        rpc = self._get_rpc(correlation_id, rpc_name, message.get('raw', ''))
        if rpc is None:
            return

        if action == actions.ACK:
            rpc.ack()
        elif action == actions.RESPONSE:
            rpc.respond(data[2])
            del self._rpcs[correlation_id]
        elif action == actions.ERROR:
            message['processedError'] = True
            rpc.error(data[0])
            del self._rpcs[correlation_id]

    def _reprovide(self):
        for rpc_name in self._providers:
            self._connection.send_message(topic_constants.RPC,
                                          actions.SUBSCRIBE,
                                          [rpc_name])
