from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals
from pyee import EventEmitter
from collections import deque
from deepstreampy.record import RecordHandler
from deepstreampy.message import message_builder, message_parser
from deepstreampy.constants import connection_state, topic, actions
from deepstreampy.constants import event as event_constants
from deepstreampy.constants import message as message_constants

from tornado import ioloop, tcpclient, concurrent, websocket
import socket
import errno


class _Connection(object):

    def __init__(self, client, url):
        self._io_loop = ioloop.IOLoop.current()

        self._client = client
        self._url = url
        self._stream = None

        self._auth_params = None
        self._auth_callback = None
        self._auth_future = None
        self._connect_callback = None
        self._connect_error = None

        self._message_buffer = ""
        self._deliberate_close = False
        self._too_many_auth_attempts = False
        self._queued_messages = deque()
        self._reconnect_timeout = None
        self._reconnection_attempt = 0
        self._current_packet_message_count = 0
        self._send_next_packet_timeout = None
        self._current_message_reset_timeout = None
        self._state = connection_state.CLOSED

    def connect(self, callback=None):
        self._connect_callback = callback
        connect_future = websocket.websocket_connect(
            self._url,
            self._io_loop,
            callback=self._on_open,
            on_message_callback=self._on_data)
        return connect_future

    def _on_open(self, f):
        exception = f.exception()
        if exception:
            self._connect_error = exception
            self._on_error(self._connect_error)

            self._reconnect_timeout = None
            self._try_reconnect()

            return

        self._stream = f.result()
        self._set_state(connection_state.AWAITING_CONNECTION)

        if self._connect_callback:
            self._connect_callback()

    def _on_error(self, error):
        self._set_state(connection_state.ERROR)

        if error.errno in (errno.ECONNRESET, errno.ECONNREFUSED):
            msg = "Can't connect! Deepstream server unreachable on " + self._url
        else:
            msg = str(error)
        self._client._on_error(topic.CONNECTION,
                               event_constants.CONNECTION_ERROR, msg)

    def start(self):
        self._io_loop.start()

    def stop(self):
        self._io_loop.stop()

    def close(self):
        self._deliberate_close = True
        self._stream.close()

    def authenticate(self, auth_params, callback):
        self._auth_params = auth_params
        self._auth_callback = callback
        self._auth_future = concurrent.Future()

        if self._too_many_auth_attempts:
            msg = "this client's connection was closed"
            self._client._on_error(topic.ERROR, event_constants.IS_CLOSED, msg)
            self._auth_future.set_result(
                    {'success': False,
                     'error': event_constants.IS_CLOSED,
                     'message': msg})

        elif self._deliberate_close and self._state == connection_state.CLOSED:
            self._connect()
            self._deliberate_close = False
            self._client.once(event_constants.CONNECTION_STATE_CHANGED,
                              lambda: self.authenticate(auth_params, callback))

        if self._state == connection_state.AWAITING_AUTHENTICATION:
            self._send_auth_params()

        return self._auth_future

    def _send_auth_params(self):
        self._set_state(connection_state.AUTHENTICATING)
        raw_auth_message = message_builder.get_message(topic.AUTH,
                                                       actions.REQUEST,
                                                       [self._auth_params])
        self._stream.write_message(raw_auth_message.encode())

    def _handle_auth_response(self, message):
        message_data = message['data']
        message_action = message['action']
        data_size = len(message_data)
        if message_action == actions.ERROR:
            if (message_data and
                    message_data[0] == event_constants.TOO_MANY_AUTH_ATTEMPTS):
                self._deliberate_close = True
                self._too_many_auth_attempts = True
            else:
                self._set_state(connection_state.AWAITING_AUTHENTICATION)

            auth_data = (self._get_auth_data(message_data[1]) if
                         data_size > 1 else None)

            if self._auth_callback:
                self._auth_callback(False,
                                    message_data[0] if data_size else None,
                                    auth_data)
            if self._auth_future:
                self._auth_future.set_result(
                    {'success': False,
                     'error': message_data[0] if data_size else None,
                     'message': auth_data})

        elif message_action == actions.ACK:
            self._set_state(connection_state.OPEN)

            auth_data = (self._get_auth_data(message_data[0]) if
                         data_size else None)

            # Resolve auth future and callback
            if self._auth_future:
                self._auth_future.set_result(
                    {'success': True, 'error': None, 'message': auth_data})

            if self._auth_callback:
                self._auth_callback(True, None, auth_data)

            self._send_queued_messages()

    def _handle_connection_response(self, message):
        action = message['action']
        data = message['data']
        if action == actions.PING:
            ping_response = message_builder.get_message(
                topic.CONNECTION, actions.PONG)
            self.send(ping_response)
        elif action == actions.ACK:
            self._set_state(connection_state.AWAITING_AUTHENTICATION)
            if self._auth_params is not None:
                self._send_auth_params()
        elif action == actions.CHALLENGE:
            self._set_state(connection_state.CHALLENGING)
            challenge_response = message_builder.get_message(
                topic.CONNECTION,
                actions.CHALLENGE_RESPONSE,
                [self._url])
            self.send(challenge_response)
        elif action == actions.REJECTION:
            self._challenge_denied = True
            self.close()
        elif action == actions.REDIRECT:
            self._url = data[0]
            self._redirecting = True
            self.close()
        elif action == actions.ERROR:
            if data[0] == event_constants.CONNECTION_AUTHENTICATION_TIMEOUT:
                self._deliberate_close = True
                self._connection_auth_timeout = True
                self._client._on_error(topic.CONNECTION, data[0], data[1])


    def _get_auth_data(self, data):
        if data:
            return message_parser.convert_typed(data, self._client)

    def _set_state(self, state):
        self._state = state
        self._client.emit(event_constants.CONNECTION_STATE_CHANGED, state)

    @property
    def state(self):
        """str: State of the connection.

        The possible states are defined in constants.connection_state.
        Can be one of the following:
            - closed
            - awaiting_authentication
            - authenticating
            - open
            - error
            - reconnecting
        """
        return self._state

    def send_message(self, topic, action, data):
        message = message_builder.get_message(topic, action, data)
        self.send(message)

    def send(self, raw_message):
        """Main method for sending messages.

        All messages are passed onto and handled by tornado.
        """
        if self._state == connection_state.OPEN:
            self._stream.write_message(raw_message.encode())
        else:
            self._queued_messages.append(raw_message.encode())

    def _send_queued_messages(self):
        if self._state != connection_state.OPEN:
            return

        while self._queued_messages:
            raw_message = self._queued_messages.popleft()
            self._stream.write_message(raw_message)

    def _on_data(self, data):
        if data is None:
            self._on_close()

        full_buffer = self._message_buffer + data
        split_buffer = full_buffer.rsplit(message_constants.MESSAGE_SEPERATOR,
                                          1)
        if len(split_buffer) > 1:
            self._message_buffer = split_buffer[1]

        raw_messages = split_buffer[0]

        parsed_messages = message_parser.parse(raw_messages, self._client)

        for msg in parsed_messages:
            if msg is None:
                continue

            if msg['topic'] == topic.CONNECTION:
                self._handle_connection_response(msg)
            elif msg['topic'] == topic.AUTH:
                self._handle_auth_response(msg)
            else:
                self._client._on_message(parsed_messages[0])

    def _try_reconnect(self):
        if self._reconnect_timeout is not None:
            return

        # TODO: Use options to set max reconnect attempts
        if self._reconnection_attempt < 3:
            self._set_state(connection_state.RECONNECTING)
            # TODO: Use options to set reconnect attempt interval
            self._reconnect_timeout = self._io_loop.call_later(
                3 * self._reconnection_attempt, self.connect)
            self._reconnection_attempt += 1
        else:
            self._clear_reconnect()

    def _clear_reconnect(self):
        self._io_loop.remove_callout(self._reconnect_timeout)
        self._reconnect_timeout = None
        self._reconnection_attempt = 0

    def _on_close(self):
        if self._deliberate_close:
            self._set_state(connection_state.CLOSED)
        else:
            self._try_reconnect()


class Client(EventEmitter, object):
    """
    deepstream.io Python client based on tornado.
    """

    def __init__(self, url):
        """Creates the client but doesn't connect to the server.

        Args:
            url (str): The url to connect to
        """
        super(Client, self).__init__()
        self._connection = _Connection(self, url)
        self._record = RecordHandler({}, self._connection, self)
        self._message_callbacks = dict()

        def not_implemented_callback(topic):
            raise NotImplementedError("Topic " + topic + " not yet implemented")

        self._message_callbacks[
            topic.EVENT] = lambda x: not_implemented_callback(topic.EVENT)

        self._message_callbacks[
            topic.RPC] = lambda x: not_implemented_callback(topic.RPC)

        self._message_callbacks[
            topic.RECORD] = self._record._handle

        self._message_callbacks[topic.ERROR] = self._on_error

    def connect(self, callback=None):
        """Establishes connection to the host and port given to the constructor.

        Args:
            callback (callable): Will be called when connection is established
                without any arguments
        """
        return self._connection.connect(callback)

    def start(self):
        self._connection.start()

    def stop(self):
        self._connection.stop()

    def close(self):
        self._connection.close()

    def login(self, auth_params, callback=None):
        """Sends authentication parameters to the server.

        If the connection is not yet established the authentication parameter
        will be stored and send once it becomes available
        Can be called multiple times until either the connection is
        authenticated.

        Note: A max auth attempts option is currently not implemented but will
            be in the near future.

        Args:
            auth_params: JSON serializable data structure, it's up to the
                permission handler on the server to make sense of them, although
                something like { username: 'someName', password: 'somePass' }
                will probably make the most sense.
            callback (callable): Will be called with True in case of success, or
                False, error_type, error_message in case of failure
        """
        return self._connection.authenticate(auth_params, callback)

    def _on_message(self, message):
        if message['topic'] in self._message_callbacks:
            self._message_callbacks[message['topic']](message)
        else:
            self._on_error(message['topic'],
                           event_constants.MESSAGE_PARSE_ERROR,
                           ('Received message for unknown topic ' +
                            message['topic']))
            return

        if message['action'] == actions.ERROR:
            self._on_error(message['topic'],
                           message['action'],
                           message['data'][0] if len(message['data']) else None)

    def _on_error(self, topic, event, msg=None):
        if event in (event_constants.ACK_TIMEOUT,
                     event_constants.RESPONSE_TIMEOUT):
            if (self._connection.state ==
                    connection_state.AWAITING_AUTHENTICATION):
                error_msg = ('Your message timed out because you\'re not '
                             'authenticated. Have you called login()?')
                self._connection._io_loop.call_later(
                    0.1, lambda: self._on_error(event.NOT_AUTHENTICATED,
                                                topic.ERROR,
                                                error_msg)
                )

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
    def io_loop(self):
        return self._connection._io_loop
