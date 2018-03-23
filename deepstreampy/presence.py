from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy.constants import topic as topic_constants
from deepstreampy.constants import actions as action_constants
from deepstreampy.constants import event as event_constants
from deepstreampy.utils import AckTimeoutRegistry
from deepstreampy.utils import ResubscribeNotifier

from tornado import concurrent

import json


class PresenceHandler(object):
    def __init__(self, connection, client, **options):
        self._options = options
        self._connection = connection
        self._client = client
        self._callbacks = {}
        self._query_callback = None
        subscription_timeout = options.get("subscriptionTimeout", 15)
        self._ack_timeout_registry = AckTimeoutRegistry(
            client, topic_constants.PRESENCE, subscription_timeout)
        self._resubscribe_notifier = ResubscribeNotifier(
            client, self._resubscribe)

    def get_all(self, callback):
        self._query_callback = callback
        return self._connection.send_message(topic_constants.PRESENCE,
                                             action_constants.QUERY,
                                             [action_constants.QUERY])

    def get(self, callback, users):
        self._query_callback = callback
        return self._connection.send_message(topic_constants.PRESENCE,
                                             action_constants.QUERY, users)

    def subscribe(self, callback, users=None):
        if users is None:
            self._callbacks[topic_constants.PRESENCE] = callback
            users_str = topic_constants.PRESENCE
        else:
            for user in users:
                self._callbacks[user] = callback
            users_str = ",".join(users)

        self._ack_timeout_registry.add(action_constants.SUBSCRIBE, users_str)

        return self._connection.send_message(
            topic_constants.PRESENCE, action_constants.SUBSCRIBE, users_str)

    def unsubscribe(self, callback, users=None):
        users_str = ""

        if users is None:
            del self._callbacks[topic_constants.PRESENCE]
        else:
            for user in users:
                del self._callbacks[user]
            users_str = ",".join(users)

        self._ack_timeout_registry.add(action_constants.UNSUBSCRIBE, users_str)

        return self._connection.send_message(
            topic_constants.PRESENCE, action_constants.UNSUBSCRIBE, users_str)

    def handle(self, message):
        action = message['action']
        data = message['data']
        if (action == action_constants.ERROR
                and data[0] == event_constants.MESSAGE_DENIED):
            self._ack_timeout_registry.remove(topic_constants.PRESENCE,
                                              data[1])
            message['processedError'] = True
            self._client._on_error(topic_constants.PRESENCE,
                                   event_constants.MESSAGE_DENIED, data[1])
        elif action == action_constants.ACK:
            self._ack_timeout_registry.clear(message)
        elif action == action_constants.PRESENCE_JOIN:
            user = data[0]
            if user in self._callbacks.keys():
                self._callbacks[user](user, True)
            if topic_constants.PRESENCE in self._callbacks:
                self._callbacks[topic_constants.PRESENCE](user, True)
        elif action == action_constants.PRESENCE_LEAVE:
            user = data[0]
            if user in self._callbacks.keys():
                self._callbacks[user](user, False)
            if topic_constants.PRESENCE in self._callbacks:
                self._callbacks[topic_constants.PRESENCE](user, False)
        elif action == action_constants.QUERY:
            parsed = self._parse_query_response(data)
            if self._query_callback:
                self._query_callback(parsed)
                self._query_callback = None
        else:
            self._client._on_error(topic_constants.PRESENCE,
                                   event_constants.UNSOLICITED_MESSAGE, action)

    def _parse_query_response(self, response):
        if response and response[0].isdigit():
            data = json.loads(response[1])
            return data
        return response

    def _resubscribe(self):
        if self._callbacks:
            self._connection.send_message(topic_constants.PRESENCE,
                                          action_constants.SUBSCRIBE,
                                          [action_constants.SUBSCRIBE])
