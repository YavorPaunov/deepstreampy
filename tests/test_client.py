"""
Tests for connecting to a client, logging in, state changes, and whether the
respective callbacks are made, and events triggered.
"""
from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy.message import connection
from deepstreampy import constants

from tornado import testing

import sys
import errno

if sys.version_info[0] < 3:
    import mock
else:
    from unittest import mock

URL = "ws://localhost:7777/deepstream"

test_server_exceptions = []


class ConnectionTest(testing.AsyncTestCase):

    def setUp(self):
        super(ConnectionTest, self).setUp()
        self.client = mock.Mock()
        self.handler = mock.Mock()

    def _get_connection_state_changes(self):
        count = 0
        for call_args in self.client.emit.call_args_list:
            if call_args[0][0] == constants.event.CONNECTION_STATE_CHANGED:
                count += 1
        return count

    def _get_sent_messages(self):
        for call_args in self.handler.write_message.call_args_list:
            yield call_args[0]

    def _get_last_sent_message(self):
        return self.handler.write_message.call_args[0][0]

    def test_connects(self):
        conn = connection.Connection(self.client, URL)
        assert conn.state == constants.connection_state.CLOSED
        self.assertEquals(self._get_connection_state_changes(), 0)
        connect_future = mock.Mock()
        connect_future_config = {'exception.return_value': None,
                                 'result.return_value': self.handler}
        connect_future.configure_mock(**connect_future_config)
        connect_future.exception.return_value = None
        connect_future.get_result.return_value = self.handler

        conn._on_open(connect_future)
        self.handler.stream.closed = mock.Mock(return_value=False)
        self.assertTrue(conn._websocket_handler is self.handler)
        self.assertEquals(conn.state,
                          constants.connection_state.AWAITING_CONNECTION)
        self.assertEquals(self._get_connection_state_changes(), 1)
        conn._on_data('C{0}A{1}'.format(chr(31), chr(30)))
        self.assertEquals(conn.state,
                          constants.connection_state.AWAITING_AUTHENTICATION)
        self.handler.write_message.assert_not_called()

        conn.authenticate({'user': 'Anon'})
        self.assertEquals(conn.state,
                          constants.connection_state.AUTHENTICATING)

        self.assertEquals(self._get_last_sent_message(),
                          "A{0}REQ{0}{{\"user\":\"Anon\"}}{1}".format(
                              chr(31), chr(30)).encode())
        self.assertEquals(self._get_connection_state_changes(), 3)

        conn._on_data('A{0}A{1}'.format(chr(31), chr(30)))
        self.assertEquals(conn.state,
                          constants.connection_state.OPEN)
        self.assertEquals(self._get_connection_state_changes(), 4)

        conn.send_message('R', 'S', ['test1'])
        self.handler.write_message.assert_called_with(
            'R{0}S{0}test1{1}'.format(chr(31), chr(30)).encode())

        conn.close()
        conn._on_close()

        self.assertEquals(conn.state,
                          constants.connection_state.CLOSED)
        self.assertEquals(self._get_connection_state_changes(), 5)

    def test_connect_error(self):
        conn = connection.Connection(self.client, URL)
        assert conn.state == constants.connection_state.CLOSED
        self.assertEquals(self._get_connection_state_changes(), 0)
        connect_future = mock.Mock()
        connect_future_config = {'exception.return_value': None,
                                 'result.return_value': self.handler}
        connect_future.configure_mock(**connect_future_config)
        connect_future.exception.return_value = IOError(
            (errno.ECONNREFUSED, "Connection refused"))
        connect_future.get_result.return_value = self.handler

        conn._on_open(connect_future)
        self.assertEquals(conn.state,
                          constants.connection_state.RECONNECTING)
        self.assertTrue(conn._reconnect_timeout is not None)
        conn._on_open(connect_future)
        conn._on_open(connect_future)
        conn._on_open(connect_future)
        self.assertEquals(conn.state,
                          constants.connection_state.ERROR)

    def test_too_many_auth_attempts(self):
        conn = connection.Connection(self.client, URL)
        connect_future = mock.Mock()
        connect_future_config = {'exception.return_value': None,
                                 'result.return_value': self.handler}
        connect_future.configure_mock(**connect_future_config)
        connect_future.exception.return_value = None
        connect_future.get_result.return_value = self.handler

        conn._on_open(connect_future)
        self.handler.stream.closed = mock.Mock(return_value=False)
        conn._on_data('C{0}A{1}'.format(chr(31), chr(30)))
        self.handler.write_message.assert_not_called()

        conn.authenticate({'user': 'Anon'})
        conn._on_data('A{0}E{1}'
                      .format(chr(31), chr(30)))
        conn.authenticate({'user': 'Anon'})
        conn._on_data('A{0}E{0}TOO_MANY_AUTH_ATTEMPTS{1}'
                      .format(chr(31), chr(30)))
        self.assertTrue(conn._too_many_auth_attempts)

if __name__ == '__main__':
    testing.unittest.main()
