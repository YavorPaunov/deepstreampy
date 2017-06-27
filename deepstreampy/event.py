"""Deepstream event handling."""

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

from tornado import concurrent

from pyee import EventEmitter


class EventHandler(object):
    """Handles incoming and outgoing messages related to deepstream events.
    """

    def __init__(self, connection, client, **options):
        self._options = options
        self._connection = connection
        self._client = client
        self._emitter = EventEmitter()
        self._listener = {}

        subscription_timeout = options.get("subscriptionTimeout", 15)
        self._ack_timeout_registry = AckTimeoutRegistry(client,
                                                        topic_constants.EVENT,
                                                        subscription_timeout)
        self._resubscribe_notifier = ResubscribeNotifier(client,
                                                         self._resubscribe)

    def subscribe(self, name, callback):
        """Subscribe to an event.

        Adds a callback for both locally emited events as well as events emitted
        by other clients.

        Args:
            name (str): The name of the event.
            callback (callable): The function to call when an event is received.

        """
        future = None
        if not self._emitter.listeners(name):
            self._ack_timeout_registry.add(name, actions.SUBSCRIBE)
            future = self._connection.send_message(topic_constants.EVENT,
                                                   actions.SUBSCRIBE,
                                                   [name])
        else:
            future = concurrent.Future()
            future.set_result(None)

        self._emitter.on(name, callback)

        return future

    def unsubscribe(self, name, callback):
        """Unsubscribe from an event.

        Removes the callback for the specified event, and notifies the server
        of the change.

        Args:
            name (str): The name of the event
            callback (callable): The callback to remove

        """
        self._emitter.remove_listener(name, callback)

        if not self._emitter.listeners(name):
            self._ack_timeout_registry.add(name, actions.UNSUBSCRIBE)
            return self._connection.send_message(topic_constants.EVENT,
                                                 actions.UNSUBSCRIBE,
                                                 [name])

        future = concurrent.Future()
        future.set_result(None)
        return future

    def emit(self, name, data):
        """Emit an event locally, and tell the server to broadcast it.

        Other connected clients will also receive the event.

        Args:
            name (str): The name of the event.
            data: JSON serializable data to send along with the event.

        """
        future = self._connection.send_message(
            topic_constants.EVENT, actions.EVENT,
            [name, message_builder.typed(data)])

        self._emitter.emit(name, data)

        return future

    def listen(self, pattern, callback):
        """Register as listener for event subscriptions from other clients.

        Args:
            pattern (str): Regular expression pattern to match subscriptions to
            callback (callable): A function that will be called when an event
                has been initially subscribed to or is no longer subscribed.

                Expects the following arguments:
                    event_name (str)
                    is_subscribed (bool)
                    response (callable, callable)

        """
        if (pattern in self._listener and
                not self._listener[pattern].destroy_pending):
            self._client._on_error(topic_constants.EVENT,
                                   event_constants.LISTENER_EXISTS,
                                   pattern)
            future = concurrent.Future()
            future.set_result(None)
            return future

        elif pattern in self._listener:
            self._listener[pattern].destroy()

        listener = Listener(topic_constants.EVENT,
                            pattern,
                            callback,
                            self._options,
                            self._client,
                            self._connection)
        self._listener[pattern] = listener

        return listener.send_future

    def unlisten(self, pattern):
        """Stop listening to the specified pattern.

        Remove a previously registered listening pattern. The client will no
        longer be listening for active/inactive subscriptions.

        Args:
            pattern: The regular expression pattern to remove
        """
        if pattern not in self._listener:
            self._client._on_error(topic_constants.ERROR,
                                   event_constants.NOT_LISTENING,
                                   pattern)
            future = concurrent.Future()
            future.set_result(None)
            return future

        listener = self._listener[pattern]

        if not listener.destroy_pending:
            listener.send_destroy()
        else:
            self._ack_timeout_registry.add(pattern, actions.UNLISTEN)
            listener.destroy()
            del self._listener[pattern]

        return listener.send_future

    def handle(self, message):
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
        elif action in (actions.SUBSCRIPTION_FOR_PATTERN_REMOVED,
                        actions.SUBSCRIPTION_HAS_PROVIDER):
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
