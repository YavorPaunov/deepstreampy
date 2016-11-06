from deepstreampy.constants import actions as action_constants, event as event_constants
from deepstreampy.constants import connection_state
from functools import partial
import sys

num_types = ((int, long, float, complex) if sys.version_info < (3,) else
             (int, float, complex))
str_types = (str, unicode) if sys.version_info < (3,) else (str,)


class SingleNotifier(object):

    def __init__(self, client, connection, topic, action, timeout_duration):
        self._client = client
        self._connection = connection
        self._topic = topic
        self._action = action
        self._timeout_duration = timeout_duration

        self._requests = dict()

        self._resubscribe_notifier = ResubscribeNotifier(client,
                                                         self._resend_requests)

    def has_request(self, name):
        return name in self._requests

    def request(self, name, callback):
        if name not in self._requests:
            self._requests[name] = []
            self._connection.send_message(self._topic, self._action, [name])

        response_timeout = self._connection.io_loop.call_later(
            self._timeout_duration, partial(self._on_response_timeout, name))
        self._requests[name].append({'timeout': response_timeout,
                                     callback: callback})

    def receive(self, name, error, data):
        entries = self._requests[name]
        for entry in entries:
            self._connection._ioloop.remove_timeout(entry['timeout'])
            entry['callback'](error, data)
        del self._requests[name]

    def _on_response_timeout(self, name):
        msg = ('No response received in time for '
               '{0}|{1}|{2}').format(self._topic, self._action, name)
        self._client._on_error(self._topic,
                               event_constants.RESPONSE_TIMEOUT,
                               msg)

    def _resend_requests(self):
        for request in self._requests:
            self._connection.send_message(self._topic, self._action,
                                          [self._requests[request]])


class Listener(object):

    def __init__(self, listener_type, pattern, callback, options, client,
                 connection):
        self._type = listener_type
        self._callback = callback
        self._pattern = pattern
        self._callback = callback
        self._options = options
        self._client = client
        self._connection = connection

        # TODO: Use subscribe timeout from options
        self._ack_timeout = connection._io_loop.call_later(1,
                                                           self._on_ack_timeout)
        self._resubscribe_notifier = ResubscribeNotifier(client,
                                                         self._send_listen)
        self._send_listen()
        self._destroy_pending = False

    def send_destroy(self):
        self._destroy_pending = True
        self._connection.send_message(self._type, action_constants.UNLISTEN,
                                      [self._pattern])
        self._resubscribe_notifier.destroy()

    def destroy(self):
        self._callback = None
        self._pattern = None
        self._client = None
        self._connection = None

    def accept(self, name):
        self._connection.send_message(self._type,
                                      action_constants.LISTEN_ACCEPT,
                                      [self._pattern, name])

    def reject(self, name):
        self._connection.send_message(self._type,
                                      action_constants.LISTEN_REJECT,
                                      [self._pattern, name])

    def _create_callback_response(self, message):
        return {'accept': partial(self.accept, message['data'][1]),
                'reject': partial(self.reject, message['data'][1])}

    def _on_message(self, message):
        action = message['action']
        data = message['data']
        if action == action_constants.ACK:
            self._connection._io_loop.remove_timeout(self._ack_timeout)
        elif action == action_constants.SUBSCRIPTION_FOR_PATTERN_FOUND:
            # TODO: Show deprecated message
            self._callback(
                data[1], True, self._create_callback_response(message))
        elif action == action_constants.SUBSCRIPTION_FOR_PATTERN_REMOVED:
            self._callback(data[1], False)
        else:
            is_found = (message['action'] ==
                        action_constants.SUBSCRIPTION_FOR_PATTERN_FOUND)
            self._callback(message['data'][1], is_found)

    def _send_listen(self):
        self._connection.send_message(self._type, action_constants.LISTEN,
                                      [self._pattern])

    def _on_ack_timeout(self):
        self._client._on_error(self._type, event_constants.ACK_TIMEOUT,
                               ('No ACK message received in time for ' +
                                self._pattern))

    @property
    def destroy_pending(self):
        return self._destroy_pending


class ResubscribeNotifier(object):
    """
    Makes sure that all functionality is resubscribed on reconnect. Subscription
    is called when the connection drops - which seems counterintuitive, but in
    fact just means that the re-subscription message will be added to the queue
    of messages that need re-sending as soon as the connection is
    re-established.

    Resubscribe logic should only occur once per connection loss.
    """
    def __init__(self, client, resubscribe):
        """
        Args:
            client: The deepstream client
            resubscribe: callable to call to allow resubscribing
        """
        self._client = client
        self._resubscribe = resubscribe

        self._is_reconnecting = False
        self._client.on(event_constants.CONNECTION_STATE_CHANGED,
                        self._handle_connection_state_changes)

    def destroy(self):
        self._client.remove_listener(event_constants.CONNECTION_STATE_CHANGED,
                                     self._handle_connection_state_changes)
        self._client = None

    def _handle_connection_state_changes(self, state):
        if state == connection_state.RECONNECTING and not self._is_reconnecting:
            self._is_reconnecting = True
        elif state == connection_state.OPEN and self._is_reconnecting:
            self._is_reconnecting = False
            self._resubscribe()


def _pad_list(l, index, value):
    l.extend([value] * (index - len(l)))


class Undefined(object):
    def __repr__(self):
        return 'Undefined'
Undefined = Undefined()
