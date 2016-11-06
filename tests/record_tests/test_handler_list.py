from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy import client
from deepstreampy.record import RecordHandler
from deepstreampy.constants import connection_state
import unittest
import sys

if sys.version_info.major < 3:
    import mock
else:
    from unittest import mock

HOST = "localhost"
PORT = 6026


class RecordTest(unittest.TestCase):

    def setUp(self):
        super(RecordTest, self).setUp()
        self.client = client.Client(HOST, PORT)
        self.iostream = mock.Mock()
        self.client._connection._state = connection_state.OPEN
        self.client._connection._stream = self.iostream
        self.connection = self.client._connection
        self.options = {'recordReadAckTimeout': 100, 'recordReadTimeout': 200}
        self.record_handler = RecordHandler({},
                                            self.client._connection,
                                            self.client)
        self.on_discard = mock.Mock()
        self.listA = self.record_handler.get_list('list_A')
        self.listA2 = self.record_handler.get_list('list_A')
        self.listA.on('discard', self.on_discard)

    def test_retrieve_list(self):
        self.iostream.write.assert_called_with(
            "R{0}CR{0}list_A{1}".format(chr(31), chr(30)).encode())

    def test_retreive_list_again(self):
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
        self.iostream.write.assert_called_with(
            "R{0}US{0}list_A{1}".format(chr(31), chr(30)).encode())

        self.record_handler._handle({
            'topic': 'R',
            'action': 'A',
            'data': ['US', 'list_A']
        })

        self.on_discard.assert_called_once()
        self.assertTrue(self.listA.is_destroyed)
        self.assertTrue(self.listA2.is_destroyed)
