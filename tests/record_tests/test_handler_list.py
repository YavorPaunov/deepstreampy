from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy import client
from deepstreampy.record import RecordHandler
from deepstreampy.constants import connection_state

from tornado import testing, concurrent

import sys
from functools import partial

if sys.version_info[0] < 3:
    import mock
else:
    from unittest import mock

URL = "ws://localhost:7777/deepstream"


class TestHandlerList(testing.AsyncTestCase):

    def setUp(self):
        super(TestHandlerList, self).setUp()
        self.client = client.Client(URL)
        self.handler = mock.Mock()
        self.handler.stream.closed = mock.Mock(return_value=False)
        self.client._connection._state = connection_state.OPEN
        self.client._connection._websocket_handler = self.handler
        self.client._io_loop = mock.Mock()

        future = concurrent.Future()
        future.set_result(None)

        self.handler.write_message = mock.Mock(
            return_value=future)
        self.record_handler = RecordHandler(self.client._connection,
                                            self.client)
        self.on_discard = mock.Mock()

        self.listA = self.io_loop.run_sync(
            partial(self.record_handler.get_list, 'list_A'))
        self.listA2 = self.io_loop.run_sync(
            partial(self.record_handler.get_list, 'list_A'))

        self.listA.on('discard', self.on_discard)

    def test_retrieve_list(self):
        self.handler.write_message.assert_called_with(
            "R{0}CR{0}list_A{1}".format(chr(31), chr(30)).encode())

    def test_retrieve_list_again(self):
        self.assertTrue(self.listA is self.listA2)

    def test_initialize_list(self):
        self.assertFalse(self.listA.is_ready)
        self.record_handler._handle({
            'topic': 'R',
            'action': 'R',
            'data': ['list_A', 0, '{}']})
        self.assertTrue(self.listA.is_ready)

    def test_discard_list(self):
        self.record_handler._handle({
            'topic': 'R',
            'action': 'R',
            'data': ['list_A', 0, '{}']})
        self.listA.discard()
        self.listA2.discard()
        self.on_discard.assert_not_called()
        self.assertFalse(self.listA.is_destroyed)
        self.handler.write_message.assert_called_with(
            "R{0}US{0}list_A{1}".format(chr(31), chr(30)).encode())

        self.record_handler._handle({
            'topic': 'R',
            'action': 'A',
            'data': ['US', 'list_A']
        })

        self.assertEquals(self.on_discard.call_count, 1)
        self.assertTrue(self.listA.is_destroyed)
        self.assertTrue(self.listA2.is_destroyed)
