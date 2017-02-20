from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy.constants import connection_state

from deepstreampy import client
from tests.util import msg

from tornado import testing
import sys

if sys.version_info[0] < 3:
    import mock
else:
    from unittest import mock

URL = "ws://localhost:7777/deepstream"


class PresenceHandlerTest(testing.AsyncTestCase):

    def setUp(self):
        super(PresenceHandlerTest, self).setUp()
        self.client = client.Client(URL)
        self.handler = mock.Mock()
        self.handler.stream.closed = mock.Mock(return_value=False)
        self.client._connection._state = connection_state.OPEN
        self.client._connection._websocket_handler = self.handler
        self.connection = self.client._connection
        self.io_loop = self.connection._io_loop
        self.client_errors = []

    def _stop_io_loop(self, *args, **kwargs):
        self.stop()

    def test_timeout(self):
        callback = mock.Mock(side_effect=self._stop_io_loop)
        error_callback = mock.Mock(side_effect=self._stop_io_loop)
        self.client.on('error', error_callback)

        # Send subscribe message
        self.client.presence.subscribe(callback)
        self.handler.write_message.assert_called_with(msg('U|S|S+'))

        # Emit timeout error
        self.wait()
        error_callback.assert_called_with(
            'No ACK message received in time for U', 'ACK_TIMEOUT', 'U')
        error_callback.reset_mock()

        # receive ack message for subscribe
        self.client.presence._handle({'topic': 'U',
                                      'action': 'A',
                                      'data': ['S']})
        self.wait()
        error_callback.assert_called_with('', 'UNSOLICITED_MESSAGE', 'U')
        error_callback.reset_mock()

        # Client logs in
        self.client.presence._handle({'topic': 'U',
                                      'action': 'PNJ',
                                      'data': ['Homer']})
        callback.assert_called_with('Homer', True)
        self.wait()

        # Client logs out
        self.client.presence._handle({'topic': 'U',
                                      'action': 'PNL',
                                      'data': ['Marge']})
        callback.assert_called_with('Marge', False)
        self.wait()

        # Query for clients
        self.client.presence.get_all(callback)
        self.handler.write_message.assert_called_with(msg('U|Q|Q+'))

        # Receive data for query
        self.client.presence._handle({'topic': 'U',
                                      'action': 'Q',
                                      'data': ['Marge', 'Homer', 'Bart']})
        callback.assert_called_with(['Marge', 'Homer', 'Bart'])
        self.wait()

        # Unsubscribe to client logins
        self.client.presence.unsubscribe(callback)
        self.handler.write_message.assert_called_with(msg('U|US|US+'))

        # emit ack timeout for unsubscribe
        self.wait()
        error_callback.assert_called_with(
            'No ACK message received in time for U', 'ACK_TIMEOUT', 'U')
        error_callback.reset_mock()

        # receive ack for unsubscrube
        self.client.presence._handle({'topic': 'U',
                                      'action': 'A',
                                      'data': ['US']})
        self.wait()
        error_callback.assert_called_with('', 'UNSOLICITED_MESSAGE', 'U')

        callback.reset_mock()
        error_callback.reset_mock()

        # not notified of future actions
        self.client.presence._handle({'topic': 'U',
                                      'action': 'PNJ',
                                      'data': ['Homer']})
        callback.assert_not_called()

        self.client.presence._handle({'topic': 'U',
                                      'action': 'PNL',
                                      'data': ['Homer']})
        callback.assert_not_called()
        self.client.presence._handle({'topic': 'U',
                                      'action': 'Q',
                                      'data': ['Marge', 'Homer', 'Bart']})
        callback.assert_not_called()
