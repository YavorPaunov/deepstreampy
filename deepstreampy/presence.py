from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy.constants import topic as topic_constants
from deepstreampy.constants import actions as action_constants
from deepstreampy.constants import event as event_constants
from deepstreampy.utils import AckTimeoutRegistry
from deepstreampy.utils import ResubscribeNotifier

from tornado import concurrent
from pyee import EventEmitter


class PresenceHandler(object):

    def __init__(self, connection, client, **options):
        self._options = options
        self._connection = connection
        self._client = client
        self._emitter = EventEmitter()
        self._ack_timeout_registry = AckTimeoutRegistry(
            client, topic_constants.PRESENCE, 1)
        self._resubscribe_notifier = ResubscribeNotifier(
            client, self._resubscribe)

    def get_all(self, callback):
        if not self._emitter.listeners(action_constants.QUERY):
            future = self._connection.send_message(
                topic_constants.PRESENCE,
                action_constants.QUERY,
                [action_constants.QUERY])
        else:
            future = concurrent.Future()
            future.set_result()

        self._emitter.once(action_constants.QUERY, callback)
        return future

    def subscribe(self, callback):
        if not self._emitter.listeners(topic_constants.PRESENCE):
            self._ack_timeout_registry.add(
                topic_constants.PRESENCE, action_constants.SUBSCRIBE)
            future = self._connection.send_message(topic_constants.PRESENCE,
                                                   action_constants.SUBSCRIBE,
                                                   [action_constants.SUBSCRIBE])
        else:
            future = concurrent.Future()
            future.set_result()

        self._emitter.on(topic_constants.PRESENCE, callback)
        return future

    def unsubscribe(self, callback):
        self._emitter.remove_listener(topic_constants.PRESENCE, callback)

        if not self._emitter.listeners(topic_constants.PRESENCE):
            self._ack_timeout_registry.add(topic_constants.PRESENCE,
                                           action_constants.UNSUBSCRIBE)
            future = self._connection.send_message(
                topic_constants.PRESENCE,
                action_constants.UNSUBSCRIBE,
                [action_constants.UNSUBSCRIBE])
        else:
            future = concurrent.Future()
            future.set_result()

        return future

    def _handle(self, message):
        action = message['action']
        data = message['data']

        if (action == action_constants.ERROR and
                data[0] == event_constants.MESSAGE_DENIED):
            self._ack_timeout_registry.remove(topic_constants.PRESENCE, data[1])
            message['processedError'] = True
            self._client._on_error(topic_constants.PRESENCE,
                                   event_constants.MESSAGE_DENIED,
                                   data[1])
        elif action == action_constants.ACK:
            self._ack_timeout_registry.clear(message)
        elif action == action_constants.PRESENCE_JOIN:
            self._emitter.emit(topic_constants.PRESENCE, data[0], True)
        elif action == action_constants.PRESENCE_LEAVE:
            self._emitter.emit(topic_constants.PRESENCE, data[0], False)
        elif action == action_constants.QUERY:
            self._emitter.emit(action_constants.QUERY, data)
        else:
            self._client._on_error(
                topic_constants.PRESENCE,
                event_constants.UNSOLICITED_MESSAGE,
                action)

    def _resubscribe(self):
        if self._emitter.listeners(topic_constants.PRESENCE):
            self._connection.send_message(topic_constants.PRESENCE,
                                          action_constants.SUBSCRIBE,
                                          [action_constants.SUBSCRIBE])
