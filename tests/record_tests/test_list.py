from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy import client
from deepstreampy.record import RecordHandler, List
from deepstreampy.constants import connection_state
from tests.util import msg
import unittest
import sys

if sys.version_info[0] < 3:
    import mock
else:
    from unittest import mock

HOST = "localhost"
PORT = 6026


class ListTest(unittest.TestCase):

    def setUp(self):
        self.client = client.Client(HOST, PORT)
        self.iostream = mock.Mock()
        self.client._connection._state = connection_state.OPEN
        self.client._connection._stream = self.iostream
        self.record_handler = RecordHandler(
            {}, self.client._connection, self.client)
        self.list = List(self.record_handler, 'someList', {})
        self.change_callback = mock.Mock()
        self.ready_callback = mock.Mock()
        self.list.subscribe(self.change_callback)
        self.list.when_ready(self.ready_callback)

    def test_create(self):
        self.assertNotEqual(self.list.get_entries(), None)
        self.iostream.write.assert_called_with(msg("R|CR|someList+").encode())
        self.ready_callback.assert_not_called()

    def test_empty(self):
        self.assertEqual(self.list.get_entries(), [])
        self.assertTrue(self.list.is_empty)

    def test_receive_response(self):
        self.record_handler._handle(
            {'topic': 'R', 'action': 'R',
             'data': ['someList', 1, '["entryA", "entryB"]']})
        self.assertEqual(self.list.get_entries(), ['entryA', 'entryB'])
        self.assertEquals(self.ready_callback.call_count, 1)
        self.change_callback.assert_called_with(['entryA', 'entryB'])
        self.assertFalse(self.list.is_empty)

    def test_append(self):
        self.record_handler._handle(
            {'topic': 'R', 'action': 'R',
             'data': ['someList', 1, '["entryA", "entryB"]']})
        self.list.add_entry('entryC')
        self.change_callback.assert_called_with(['entryA', 'entryB', 'entryC'])
        self.assertEqual(self.list.get_entries(),
                             ['entryA', 'entryB', 'entryC'])
        self.iostream.write.assert_called_with(
            msg('R|U|someList|2|["entryA","entryB","entryC"]+').encode())

    def test_remove(self):
        self.record_handler._handle(
           {'topic': 'R', 'action': 'R',
            'data': ['someList', 1, '["entryA", "entryB"]']})
        self.assertEqual(self.list.get_entries(), ['entryA', 'entryB'])
        self.list.remove_entry('entryB')
        self.change_callback.assert_called_with(['entryA'])
        self.assertEqual(self.list.get_entries(), ['entryA'])
        self.iostream.write.assert_called_with(
            msg('R|U|someList|2|["entryA"]+').encode())

    def test_insert(self):
        self.record_handler._handle(
            {'topic': 'R', 'action': 'R',
             'data': ['someList', 1, '["entryA", "entryB"]']})
        self.list.add_entry('entryC', 1)
        self.change_callback.assert_called_with(['entryA', 'entryC', 'entryB'])
        self.assertEqual(self.list.get_entries(),
                             ['entryA', 'entryC', 'entryB'])
        self.iostream.write.assert_called_with(
            msg('R|U|someList|2|["entryA","entryC","entryB"]+').encode())

    def test_remove_at_index(self):
        self.record_handler._handle(
            {'topic': 'R', 'action': 'R',
             'data': ['someList', 1, '["entryA", "entryB", "entryC"]']})
        self.list.remove_at(1)
        self.change_callback.assert_called_with(['entryA', 'entryC'])
        self.iostream.write.assert_called_with(
            msg('R|U|someList|2|["entryA","entryC"]+').encode())

    def test_set_entire_list(self):
        self.record_handler._handle(
            {'topic': 'R', 'action': 'R',
             'data': ['someList', 1, '["entryA", "entryB", "entryC"]']})
        self.list.set_entries(['x', 'y'])
        self.assertEqual(self.list.get_entries(), ['x', 'y'])
        self.change_callback.assert_called_with(['x', 'y'])

    def test_server_update(self):
        self.record_handler._handle(
           {'topic': 'R', 'action': 'R',
            'data': ['someList', 1, '["entryA", "entryB"]']})
        self.record_handler._handle({'topic': 'R',
                                     'action': 'R',
                                     'data': ['someList', 2, '["x","y"]']})
        self.change_callback.assert_called_with(['x', 'y'])
        self.assertEquals(self.list.version, 2)
        self.assertEqual(self.list.get_entries(), ['x', 'y'])

    def test_empty_list(self):
        self.record_handler._handle(
           {'topic': 'R', 'action': 'R', 'data': ['someList', 1, '[]']})
        self.assertEqual(self.list.get_entries(), [])
        self.assertTrue(self.list.is_empty)

        self.list.add_entry('entry')
        self.assertEqual(self.list.get_entries(), ['entry'])
        self.assertFalse(self.list.is_empty)

        self.list.remove_entry('entry')
        self.assertEqual(self.list.get_entries(), [])
        self.assertTrue(self.list.is_empty)

    def test_unsubscribe(self):
        self.change_callback.reset_mock()
        self.list.unsubscribe(self.change_callback)
        self.list.set_entries(['q'])
        self.change_callback.assert_not_called()
