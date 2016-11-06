from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy import client
from deepstreampy.record import RecordHandler
from deepstreampy.constants import connection_state
import unittest
import mock

HOST = "localhost"
PORT = 6026


class TestRecordRead(unittest.TestCase):

    def setUp(self):
        self.client = client.Client(HOST, PORT)
        self.iostream = mock.Mock()
        self.client._connection._state = connection_state.OPEN
        self.client._connection._stream = self.iostream
        self.client._io_loop = mock.Mock()
        self.record_handler = RecordHandler({},
                                            self.client._connection,
                                            self.client)
        self.on_discard = mock.Mock()
        self.record_A = self.record_handler.get_record('record_A')
        self.record_A.on('discard', self.on_discard)

    def _initialise(self):
        self.record_handler._handle({
            'topic': 'R',
            'action': 'R',
            'data': ['record_A', 0, '{}']})

    def test_retrieve(self):
        self.iostream.write.assert_called_with("R{0}CR{0}record_A{1}"
                                               .format(chr(31), chr(30)))

    def test_initialise(self):
        self.assertFalse(self.record_A.is_ready)
        self._initialise()
        self.assertTrue(self.record_A.is_ready)

    def test_discard(self):
        self._initialise()
        self.record_A.discard()
        self.on_discard.assert_not_called()
        self.assertFalse(self.record_A.is_destroyed)
        self.iostream.write.assert_called_with("R{0}US{0}record_A{1}"
                                               .format(chr(31), chr(30)))

    def test_resubscribe(self):
        self._initialise()
        self.record_A.discard()
        self.assertFalse(self.record_A.is_destroyed)
        self.assertIsNot(self.record_handler.get_record('record_A'),
                         self.record_A)

    def test_discard_ack(self):
        self._initialise()
        self.record_A.discard()
        self.record_handler._handle({'topic': 'R', 'action': 'A',
                                     'data': ['US', 'record_A']})
        self.on_discard.assert_called_once()
        self.assertTrue(self.record_A.is_destroyed)

    def tearDown(self):
        self.iostream.reset_mock()
        self.iostream.write.reset_mock()


class TestRecordDeleted(unittest.TestCase):

    def _create_empty(self):
        self.record_handler._handle({
            'topic': 'R',
            'action': 'R',
            'data': ['record_A', 0, '{}']})

    def _delete(self):
        self.record_handler._handle({
            'topic': 'R',
            'action': 'A',
            'data': ['D', 'record_A']})

    def setUp(self):
        self.client = client.Client(HOST, PORT)
        self.iostream = mock.Mock()
        self.client._connection._state = connection_state.OPEN
        self.client._connection._stream = self.iostream
        self.client._io_loop = mock.Mock()
        self.record_handler = RecordHandler({},
                                            self.client._connection,
                                            self.client)
        self.on_delete = mock.Mock()
        self.record_A = self.record_handler.get_record('record_A')
        self.record_A.on('delete', self.on_delete)

    def test_retrieve(self):
        self.iostream.write.assert_called_with(
            "R{0}CR{0}record_A{1}".format(chr(31), chr(30)))

    def test_initialize(self):
        self.assertFalse(self.record_A.is_ready)
        self._create_empty()
        self.assertTrue(self.record_A.is_ready)

    def test_receive_delete(self):
        self._create_empty()

        self.on_delete.assert_not_called()
        self.assertFalse(self.record_A.is_destroyed)

        self._delete()

        self.on_delete.assert_called_once()
        self.assertTrue(self.record_A.is_destroyed)

    def test_resubscribe(self):
        self._create_empty()
        self._delete()

        self.on_delete.assert_called_once()
        self.assertTrue(self.record_A.is_destroyed)

        new_record = self.record_handler.get_record('record_A')
        self.assertFalse(new_record is self.record_A)
        self.iostream.write.assert_called_with(
            "R{0}CR{0}record_A{1}".format(chr(31), chr(30)))
