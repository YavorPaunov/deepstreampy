from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy.constants import actions
from deepstreampy.constants import topic as topic_constants
from deepstreampy.constants import event as event_constants
from deepstreampy.message import message_parser
from deepstreampy.message import message_builder
from deepstreampy.utils import Listener
from deepstreampy.utils import AckTimeoutRegistry
from deepstreampy.utils import ResubscribeNotifier

from pyee import EventEmitter


class EventHandler(object):

    def __init__(self, connection, client, **options):
        self._options = options
        self._connection = connection
        self._client = client
        self._emitter = EventEmitter()
        self._listener = {}
        # TODO: Use options to set subscriptionTimeout
        self._ack_timeout_registry = AckTimeoutRegistry(client,
                                                        topic_constants.EVENT,
                                                        1)
        self._resubscribe_notifier = ResubscribeNotifier(client,
                                                         self._resubscribe)

    def subscribe(self, name, callback):
        if not self._emitter.listeners(name):
            self._ack_timeout_registry.add(name, actions.SUBSCRIBE)
            self._connection.send_message(topic_constants.EVENT,
                                          actions.SUBSCRIBE,
                                          [name])

        self._emitter.on(name, callback)

    def unsubscribe(self, name, callback):
        self._emitter.remove_listener(name, callback)

        if not self._emitter.listeners(name):
            self._ack_timeout_registry.add(name, actions.UNSUBSCRIBE)
            self._connection.send_message(topic_constants.EVENT,
                                          actions.UNSUBSCRIBE,
                                          [name])

    def emit(self, name, data):
        self._connection.send_message(topic_constants.EVENT,
                                      actions.EVENT,
                                      [name, message_builder.typed(data)])
        self._emitter.emit(name, data)

    def listen(self, pattern, callback):
        if (pattern in self._listener and
                not self._listener[pattern].destroy_pending):
            return self._client._on_error(topic_constants.EVENT,
                                          event_constants.LISTENER_EXISTS,
                                          pattern)
        elif pattern in self._listener:
            self._listener[pattern].destroy()

        self._listener[pattern] = Listener(topic_constants.EVENT,
                                           pattern,
                                           callback,
                                           self._options,
                                           self._client,
                                           self._connection)

    def unlisten(self, pattern):
        if pattern not in self._listener:
            self._client._on_error(topic_constants.ERROR,
                                   event_constants.NOT_LISTENING,
                                   pattern)
            return

        listener = self._listener[pattern]

        if not listener.destroy_pending:
            listener.send_destroy()
        else:
            self._ack_timeout_registry.add(pattern, actions.UNLISTEN)
            listener.destroy()
            del self._listener[pattern]

    def _handle(self, message):
        action = message['action']
        data = message['data']

        if action == actions.ACK:
            name = message['data'][1]
        else:
            name = message['data'][0]

        if action == actions.EVENT:
            if data and len(data) == 2:
                self._emitter.emit(
                    name, message_parser.convert_typed(data[1], self._client))
            else:
                self._emitter.emit(name)

            return

        if (action == actions.ACK and data[0] == actions.UNLISTEN and
                (name in self._listener) and
                self._listener[name].destroy_pending):
            self._listener[name].destroy()
            del self._listener[name]
            return
        elif name in self._listener:
            self._listener[name]._on_message(message)
            return
        elif action == actions.SUBSCRIPTION_FOR_PATTERN_REMOVED:
            return
        elif action == actions.SUBSCRIPTION_HAS_PROVIDER:
            return

        if action == actions.ACK:
            self._ack_timeout_registry.clear(message)
            return

        if action == actions.ERROR:
            if data[0] == event_constants.MESSAGE_DENIED:
                self._ack_timeout_registry.remove(message['data'][1],
                                                  message['data'][2])
            elif data[0] == event_constants.NOT_SUBSCRIBED:
                self._ack_timeout_registry.remove(message['data'][1],
                                                  actions.UNSUBSCRIBE)

            message['processedError'] = True
            self._client._on_error(topic_constants.EVENT, data[0], data[1])
            return

        self._client._on_error(topic_constants.EVENT,
                               event_constants.UNSOLICITED_MESSAGE,
                               name)

    def _resubscribe(self):
        for event in self._emitter._events:
            self._connection.send_message(topic_constants.EVENT,
                                          actions.SUBSCRIBE,
                                          [event])
