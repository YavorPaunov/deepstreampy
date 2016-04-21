"""
Tests for connecting to a client, logging in, state changes, and whether the
respective callbacks are made, and events triggered.
"""
from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from tests.util import FakeServer
from deepstreampy.message import message_builder, message_parser
from deepstreampy.constants import topic, actions, connection_state
from deepstreampy import client
from tornado import testing

HOST = "localhost"
PORT = 6029

test_server_exceptions = []


class ConnectionTest(testing.AsyncTestCase):

    def setUp(self):
        super(ConnectionTest, self).setUp()
        self.server = FakeServer()
        self.server.listen(PORT)
        self.server.start()
        self.client = client.Client(HOST, PORT)
        print("SETUP")

    @testing.gen_test
    def test_success_login(self):
        """Generator style test for successful login."""
        self.assertEquals(self.client.connection_state, connection_state.CLOSED)
        yield self.client.connect()

        self.assertEquals(self.client.connection_state,
                          connection_state.AWAITING_AUTHENTICATION)
        login_future = self.client.login({"username": "alice"})
        self.assertEquals(self.client.connection_state,
                          connection_state.AUTHENTICATING)
        self.server.write(message_builder.get_message(topic.AUTH, actions.ACK))
        login_data = yield login_future

        self.assertIsInstance(login_data, dict)
        self.assertTrue(login_data['success'])
        self.assertIsNone(login_data['error'])

    @testing.gen_test
    def test_login_error(self):
        """Generator style test for failed login."""
        self.assertEquals(self.client.connection_state, connection_state.CLOSED)
        yield self.client.connect()

        self.assertEquals(self.client.connection_state,
                          connection_state.AWAITING_AUTHENTICATION)
        login_future = self.client.login({"username": "alice"})
        login_error = "INVALID_AUTH_DATA"
        self.server.write(message_builder.get_message(
            topic.AUTH,
            actions.ERROR,
            [login_error, message_builder.typed('invalid user')]))
        login_data = yield login_future

        self.assertEquals(self.client.connection_state,
                          connection_state.AWAITING_AUTHENTICATION)

        self.assertFalse(login_data['success'])
        self.assertEquals(login_data['error'], login_error)
        self.assertEquals(login_data['message'],
                          message_parser.convert_typed('Sinvalid user',
                                                       self.client))

    def test_success_login_callback(self):
        """Callback style test for successful login."""
        self.assertEquals(self.client.connection_state, connection_state.CLOSED)
        self.client.connect(self.stop)
        self.wait()
        self.assertEquals(self.client.connection_state,
                          connection_state.AWAITING_AUTHENTICATION)
        self.client.login({"username": "alice"}, self._handle_success_login)
        self.assertEquals(self.client.connection_state,
                          connection_state.AUTHENTICATING)

        self.server.write(message_builder.get_message(topic.AUTH, actions.ACK))
        self.wait()

    def _handle_success_login(self, success, error, message):
        self.assertTrue(success)
        self.stop()

    def test_login_error_callback(self):
        """Callback style test for failed login."""
        self.assertEquals(self.client.connection_state, connection_state.CLOSED)
        self.client.connect(self.stop)
        self.wait()

        self.assertEquals(self.client.connection_state,
                          connection_state.AWAITING_AUTHENTICATION)
        self.client.login({"username": "alice"}, self._handle_failed_login)
        self.server.write(message_builder.get_message(
            topic.AUTH,
            actions.ERROR,
            ["INVALID_AUTH_DATA", message_builder.typed('invalid user')]))
        self.wait()

        self.assertEquals(self.client.connection_state,
                          connection_state.AWAITING_AUTHENTICATION)

    def _handle_failed_login(self, success, error, message):
        self.assertFalse(success)
        self.assertEquals(error, "INVALID_AUTH_DATA")
        self.assertEquals(message,
                          message_parser.convert_typed('Sinvalid user',
                                                       self.client))
        self.stop()

    @testing.gen_test
    def test_send_queued(self):
        """Test sending messages queued up before establishing connection."""
        login_future = self.client.login({"username": "alice"})
        self.assertEquals(self.client.connection_state,
                          connection_state.CLOSED)
        yield self.client.connect()
        self.server.write(message_builder.get_message(topic.AUTH, actions.ACK))
        result_data = yield login_future
        self.assertTrue(result_data['success'])

    def tearDown(self):
        super(ConnectionTest, self).tearDown()
        self.server.stop()

if __name__ == '__main__':
    testing.unittest.main()
