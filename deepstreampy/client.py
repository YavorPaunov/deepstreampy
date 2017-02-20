from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy.message import connection
from deepstreampy import constants
from deepstreampy.record import RecordHandler
from deepstreampy.event import EventHandler
from deepstreampy.rpc import RPCHandler
from deepstreampy.presence import PresenceHandler
from deepstreampy.utils import itoa

from pyee import EventEmitter
from tornado import gen

import time
import random


@gen.coroutine
def connect(url, **options):
    """Establish a connection to a deepstream.io server.

    Args:
        url (string): URL to connect to
        **options: The options for the client

    Returns:
        tornado.concurrent.Future: A future that resolves with an instance of
            ``deepstreampy.client.Client`` when a connection is established. If
            the client is unable to connect, the future will have the
            appropriate exception set.
    """
    client = Client(url, **options)
    yield client.connect()
    raise gen.Return(client)


class Client(EventEmitter):
    """
    deepstream.io Python client based on tornado.
    """

    def __init__(self, url, **options):
        """Creates the client but doesn't connect to the server.

        Args:
            url (str): The url to connect to
            options
        """
        super(Client, self).__init__()
        self._connection = connection.Connection(self, url, **options)
        self._presence = PresenceHandler(self._connection, self, **options)
        self._event = EventHandler(self._connection, self, **options)
        self._rpc = RPCHandler(self._connection, self, **options)
        self._record = RecordHandler(self._connection, self, **options)
        self._message_callbacks = dict()

        def not_implemented_callback(topic):
            raise NotImplementedError("Topic " + topic + " not yet implemented")

        self._message_callbacks[
            constants.topic.PRESENCE] = self._presence._handle

        self._message_callbacks[
            constants.topic.EVENT] = self._event._handle

        self._message_callbacks[
            constants.topic.RPC] = self._rpc._handle

        self._message_callbacks[
            constants.topic.RECORD] = self._record._handle

        self._message_callbacks[constants.topic.ERROR] = self._on_error

    def connect(self, callback=None):
        """Establishes a connection to the url given to the constructor.

        Args:
            callback (callable): Will be called when connection is established
                without any arguments

        Returns:
            tornado.concurrent.Future: A future that resolves when the
                connection is established, or raises an exception if the client
                is unable to connect
        """
        return self._connection.connect(callback)

    def start(self):
        self._connection.start()

    def stop(self):
        self._connection.stop()

    def close(self):
        self._connection.close()

    def login(self, auth_params):
        """Sends authentication parameters to the server.

        If the connection is not yet established the authentication parameter
        will be stored and send once it becomes available
        Can be called multiple times until either the connection is
        authenticated or the max auth attempts is reached.

        Args:
            auth_params: JSON serializable data structure, it's up to the
                permission handler on the server to make sense of them.
            callback (callable): Will be called with True in case of success, or
                False, error_type, error_message in case of failure
        """
        return self._connection.authenticate(auth_params)

    def get_uid(self):
        timestamp = itoa(int(time.time() * 1000), 36)
        random_str = itoa(int(random.random() * 10000000000000000), 36)
        return "{0}-{1}".format(timestamp, random_str)

    def _on_message(self, message):
        if message['topic'] in self._message_callbacks:
            self._message_callbacks[message['topic']](message)
        else:
            self._on_error(message['topic'],
                           constants.event.MESSAGE_PARSE_ERROR,
                           ('Received message for unknown topic ' +
                            message['topic']))
            return

        if message['action'] == constants.actions.ERROR:
            self._on_error(message['topic'],
                           message['action'],
                           message['data'][0] if len(message['data']) else None)

    def _on_error(self, topic, event, msg=None):
        if event in (constants.event.ACK_TIMEOUT,
                     constants.event.RESPONSE_TIMEOUT):
            if (self._connection.state ==
                    constants.connection_state.AWAITING_AUTHENTICATION):
                error_msg = ('Your message timed out because you\'re not '
                             'authenticated. Have you called login()?')
                self._connection._io_loop.call_later(
                    0.1, lambda: self._on_error(event.NOT_AUTHENTICATED,
                                                constants.topic.ERROR,
                                                error_msg))

        if self.listeners('error'):
            self.emit('error', msg, event, topic)
            self.emit(event, topic, msg)
        else:
            raw_error_message = event + ': ' + msg

            if topic:
                raw_error_message += ' (' + topic + ')'

            raise ValueError(raw_error_message)

    @property
    def connection_state(self):
        return self._connection.state

    @property
    def record(self):
        return self._record

    @property
    def event(self):
        return self._event

    @property
    def rpc(self):
        return self._rpc

    @property
    def presence(self):
        return self._presence

    @property
    def io_loop(self):
        return self._connection._io_loop
