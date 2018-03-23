from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy.utils import str_types
from deepstreampy import constants
from deepstreampy.message import message_builder, message_parser

from tornado import ioloop, concurrent, websocket, gen

from collections import deque
import errno
import time


class Connection(object):

    def __init__(self, client, url, **options):
        self._io_loop = ioloop.IOLoop.current()

        self._client = client
        self._original_url = url
        self._url = self._original_url
        self._websocket_handler = None

        self._auth_params = None
        self._auth_future = None
        self._connect_callback = None
        self._connect_error = None

        self._message_buffer = ""
        self._deliberate_close = False
        self._redirecting = False
        self._too_many_auth_attempts = False
        self._queued_messages = deque()
        self._reconnect_timeout = None
        self._reconnection_attempt = 0

        self._current_packet_message_count = 0
        self._send_next_packet_timeout = None
        self._last_heartbeat = None
        self._heartbeat_callback = None

        self._challenge_denied = False
        self._connection_auth_timeout = False

        self._current_message_reset_timeout = None
        self._state = constants.connection_state.CLOSED

        self._max_reconnect_attempts = options.get('maxReconnectAttempts', 3)
        self._reconnect_interval_increment = options.get(
            'reconnectAttemptInterval', 4)
        self._max_reconnect_interval = options.get(
            'maxReconnectInterval', 18)
        self._heartbeat_interval = options.get('heartbeatInterval', 100)

    def connect(self, callback=None):
        self._connect_callback = callback

        connect_future = websocket.websocket_connect(
                self._url,
                self._io_loop,
                callback=self._on_open,
                on_message_callback=self._on_data)

        return connect_future

    def _check_heartbeat(self):
        heartbeat_tolerance = self._heartbeat_interval * 2
        elapsed = time.time() - self._last_heartbeat
        if elapsed >= heartbeat_tolerance:
            self._io_loop.remove_timeout(self._heartbeat_callback)
            self._websocket_handler.close()
            self._on_error("heartbeat not received in the last 1000 milliseconds")
        else:
            self._heartbeat_callback = self._io_loop.call_later(
                self._heartbeat_interval, self._check_heartbeat)

    def _on_open(self, f):
        exception = f.exception()
        if exception:
            self._connect_error = exception
            self._on_error(self._connect_error)

            self._try_reconnect()

            return

        self._last_heartbeat = time.time()
        self._heartbeat_callback = self._io_loop.call_later(
            self._heartbeat_interval, self._check_heartbeat)

        self._websocket_handler = f.result()
        self._set_state(constants.connection_state.AWAITING_CONNECTION)

        if self._connect_callback:
            self._connect_callback()

    def _on_error(self, error):
        if self._heartbeat_callback:
            self._io_loop.remove_timeout(self._heartbeat_callback)
        self._set_state(constants.connection_state.ERROR)

        if isinstance(error, str_types):
            msg = error
        elif error.errno in (errno.ECONNRESET, errno.ECONNREFUSED):
            msg = ("Can't connect! Deepstream server unreachable on " +
                   self._url)
        else:
            msg = str(error)

        self._client._on_error(constants.topic.CONNECTION,
                               constants.event.CONNECTION_ERROR, msg)

    def start(self):
        self._io_loop.start()

    def stop(self):
        self._io_loop.stop()

    def close(self):
        if self._heartbeat_callback:
            self._io_loop.remove_timeout(self._heartbeat_callback)
        self._deliberate_close = True
        if self._websocket_handler:
            self._websocket_handler.close()

    def authenticate(self, auth_params):
        self._auth_params = auth_params
        self._auth_future = concurrent.Future()

        if (self._too_many_auth_attempts or
                self._challenge_denied or
                self._connection_auth_timeout):
            msg = "this client's connection was closed"
            self._client._on_error(constants.topic.ERROR,
                                   constants.event.IS_CLOSED,
                                   msg)
            self._auth_future.set_result(
                    {'success': False,
                     'error': constants.event.IS_CLOSED,
                     'message': msg})

        elif (self._deliberate_close and
              self._state == constants.connection_state.CLOSED):
            self.connect()
            self._deliberate_close = False
            self._client.once(constants.event.CONNECTION_STATE_CHANGED,
                              lambda: self.authenticate(auth_params))

        if self._state == constants.connection_state.AWAITING_AUTHENTICATION:
            self._send_auth_params()

        return self._auth_future

    def _send_auth_params(self):
        self._set_state(constants.connection_state.AUTHENTICATING)
        raw_auth_message = message_builder.get_message(
            constants.topic.AUTH,
            constants.actions.REQUEST,
            [self._auth_params])
        self._websocket_handler.write_message(raw_auth_message.encode())

    def _handle_auth_response(self, message):
        message_data = message['data']
        message_action = message['action']
        data_size = len(message_data)
        if message_action == constants.actions.ERROR:
            if (message_data and
                    message_data[0] == constants.event.TOO_MANY_AUTH_ATTEMPTS):
                self._deliberate_close = True
                self._too_many_auth_attempts = True
            else:
                self._set_state(
                    constants.connection_state.AWAITING_AUTHENTICATION)

            auth_data = (self._get_auth_data(message_data[1]) if
                         data_size > 1 else None)

            if self._auth_future:
                self._auth_future.set_result(
                    {'success': False,
                     'error': message_data[0] if data_size else None,
                     'message': auth_data})

        elif message_action == constants.actions.ACK:
            self._set_state(constants.connection_state.OPEN)

            auth_data = (self._get_auth_data(message_data[0]) if
                         data_size else None)

            # Resolve auth future and callback
            if self._auth_future:
                self._auth_future.set_result(
                    {'success': True, 'error': None, 'message': auth_data})

            self._send_queued_messages()

    def _handle_connection_response(self, message):
        action = message['action']
        data = message['data']
        if action == constants.actions.PING:
            self._last_heartbeat = time.time()
            ping_response = message_builder.get_message(
                constants.topic.CONNECTION, constants.actions.PONG)
            self.send(ping_response)
        elif action == constants.actions.ACK:
            self._set_state(constants.connection_state.AWAITING_AUTHENTICATION)
            if self._auth_params is not None:
                self._send_auth_params()
        elif action == constants.actions.CHALLENGE:
            challenge_response = message_builder.get_message(
                constants.topic.CONNECTION,
                constants.actions.CHALLENGE_RESPONSE,
                [self._url])
            self._set_state(constants.connection_state.CHALLENGING)
            self.send(challenge_response)
        elif action == constants.actions.REJECTION:
            self._challenge_denied = True
            self.close()
        elif action == constants.actions.REDIRECT:
            self._url = data[0]
            self._redirecting = True
            self.close()
        elif action == constants.actions.ERROR:
            if data[0] == constants.event.CONNECTION_AUTHENTICATION_TIMEOUT:
                self._deliberate_close = True
                self._connection_auth_timeout = True
                self._client._on_error(
                    constants.topic.CONNECTION, data[0], data[1])

    def _get_auth_data(self, data):
        if data:
            return message_parser.convert_typed(data, self._client)

    def _set_state(self, state):
        self._state = state
        self._client.emit(constants.event.CONNECTION_STATE_CHANGED, state)

    @property
    def state(self):
        """str: State of the connection.

        The possible states are defined in constants.connection_state:
            - closed
            - awaiting_authentication
            - authenticating
            - open
            - error
            - reconnecting
        """
        return self._state

    @property
    def io_loop(self):
        return self._io_loop

    def send_message(self, topic, action, data):
        message = message_builder.get_message(topic, action, data)
        return self.send(message)

    def send(self, raw_message):
        """Main method for sending messages.

        All messages are passed onto and handled by tornado.
        """
        if not self._websocket_handler.stream.closed():
            return self._websocket_handler.write_message(raw_message.encode())
        else:
            future = concurrent.Future()
            self._queued_messages.append((raw_message.encode(), future))
            return future

    @gen.coroutine
    def _send_queued_messages(self):
        if self._state != constants.connection_state.OPEN:
            return

        while self._queued_messages:
            raw_message, future = self._queued_messages.popleft()
            result = yield self._websocket_handler.write_message(raw_message)
            future.set_result(result)

    def _on_data(self, data):
        if data is None:
            self._on_close()
            return
        full_buffer = self._message_buffer + data
        split_buffer = full_buffer.rsplit(constants.message.MESSAGE_SEPERATOR,
                                          1)
        if len(split_buffer) > 1:
            self._message_buffer = split_buffer[1]

        raw_messages = split_buffer[0]

        parsed_messages = message_parser.parse(raw_messages, self._client)

        for msg in parsed_messages:
            if msg is None:
                continue
            if msg['topic'] == constants.topic.CONNECTION:
                self._handle_connection_response(msg)
            elif msg['topic'] == constants.topic.AUTH:
                self._handle_auth_response(msg)
            else:
                self._client._on_message(parsed_messages[0])

    def _try_reconnect(self):
        if self._reconnect_timeout is not None:
            return

        if self._reconnection_attempt < self._max_reconnect_attempts:
            self._set_state(constants.connection_state.RECONNECTING)

            interval = min(
                (self._reconnect_interval_increment *
                 self._reconnection_attempt),
                self._max_reconnect_interval)

            self._reconnection_attempt += 1

            self._reconnect_timeout = self._io_loop.call_later(
                interval, self._try_open)

        else:
            self._clear_reconnect()
            self.close()

    def _try_open(self):
        self._url = self._original_url
        self.connect()
        self._reconnect_timeout = None

    def _clear_reconnect(self):
        self._io_loop.remove_timeout(self._reconnect_timeout)
        self._reconnect_timeout = None
        self._reconnection_attempt = 0

    def _on_close(self):
        self._io_loop.remove_timeout(self._heartbeat_callback)

        if self._redirecting:
            self._redirecting = False
            self.connect()
        elif self._deliberate_close:
            self._set_state(constants.connection_state.CLOSED)
        else:
            self._try_reconnect()
