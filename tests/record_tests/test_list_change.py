from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy import client
from deepstreampy.record import RecordHandler, List
from deepstreampy.record import ENTRY_ADDED_EVENT
from deepstreampy.record import ENTRY_REMOVED_EVENT
from deepstreampy.record import ENTRY_MOVED_EVENT
from deepstreampy.constants import connection_state
import unittest
import sys

if sys.version_info[0] < 3:
    import mock
else:
    from unittest import mock

URL = "ws://localhost:7777/deepstream"


class ListChangeTest(unittest.TestCase):

    def setUp(self):
        self.client = client.Client(URL)
        self.iostream = mock.Mock()
        self.client._connection._state = connection_state.OPEN
        self.client._connection._stream = self.iostream
        self.record_handler = RecordHandler(
            {}, self.client._connection, self.client)

        self.list = List(self.record_handler, 'someList', {})
        self.record_handler._handle({'topic': 'R', 'action': 'R',
                                     'data': ['someList', 1, '{}']})

    def _create_callback(self, event_name):
        callback = mock.Mock()
        self.list.set_entries(['a', 'b', 'c', 'd', 'e'])
        self.list.once(event_name, callback)
        return callback

    def test_create(self):
        self.assertEqual(self.list.get_entries(), [])
        self.assertTrue(self.list.is_empty)
        callback = mock.Mock()

        def ready_callback(record):
            self.list.subscribe(callback)
            callback.assert_not_called()

        self.list.when_ready(ready_callback)

    def test_no_listeners(self):
        callback = mock.Mock()
        self.list.set_entries(['a', 'b', 'c', 'd', 'e'])
        self.list.subscribe(callback)
        self.list.set_entries(['a', 'b', 'c', 'd', 'e', 'f'])
        callback.assert_called_once_with(['a', 'b', 'c', 'd', 'e', 'f'])

    def test_notify_on_append(self):
        callback = self._create_callback(ENTRY_ADDED_EVENT)
        self.list.add_entry('f')
        callback.assert_called_once_with('f', 5)

    def test_notify_on_append_external(self):
        callback = self._create_callback(ENTRY_ADDED_EVENT)
        self.record_handler._handle(
            {'topic': 'R',
             'action': 'U',
             'data': ['someList', 3, '["a","b","c","d","e","x"]']})
        callback.assert_called_once_with('x', 5)

    def test_notify_on_insert(self):
        callback = self._create_callback(ENTRY_ADDED_EVENT)
        self.list.add_entry('f', 3)
        callback.assert_called_once_with('f', 3)

    def test_notify_on_remove(self):
        callback = self._create_callback(ENTRY_REMOVED_EVENT)
        self.list.remove_entry('c')
        callback.assert_called_once_with('c', 2)

    def test_notify_on_move(self):
        callback = mock.Mock()
        self.list.set_entries(['a', 'b', 'c', 'd', 'e'])
        self.list.on(ENTRY_MOVED_EVENT, callback)
        self.list.set_entries(['a', 'b', 'e', 'd', 'c'])
        self.assertEqual(callback.call_count, 2)
        callback.assert_any_call('e', 2)
        callback.assert_any_call('c', 4)

    def test_notify_on_existing_insert(self):
        callback = self._create_callback(ENTRY_ADDED_EVENT)
        self.list.add_entry('a', 3)
        callback.assert_called_once_with('a', 3)

    def test_notify_on_existing_append(self):
        callback = self._create_callback(ENTRY_ADDED_EVENT)
        self.list.add_entry('b')
        callback.assert_called_once_with('b', 5)

    def test_notify_on_remvoe_second(self):
        callback = mock.Mock()
        self.list.set_entries(['a', 'b', 'c', 'd', 'c', 'e'])
        self.list.on(ENTRY_REMOVED_EVENT, callback)
        self.list.set_entries(['a', 'b', 'c', 'd', 'e'])
        callback.assert_called_once_with('c', 4)

    def test_notify_on_move_remove(self):
        remove_callback = mock.Mock()
        move_callback = mock.Mock()
        self.list.set_entries(['a', 'b', 'c', 'd', 'e'])
        self.list.on(ENTRY_MOVED_EVENT, move_callback)
        self.list.on(ENTRY_REMOVED_EVENT, remove_callback)
        self.list.set_entries(['a', 'd', 'b', 'c'])

        move_callback.assert_any_call('d', 1)
        move_callback.assert_any_call('b', 2)
        move_callback.assert_any_call('c', 3)
        remove_callback.assert_any_call('e', 4)
        self.assertEqual(move_callback.call_count, 3)
        self.assertEqual(remove_callback.call_count, 1)

    def test_notify_on_add_move_remvoe(self):
        remove_callback = mock.Mock()
        move_callback = mock.Mock()
        add_callback = mock.Mock()

        self.list.set_entries(['a', 'b', 'c', 'd', 'e'])
        self.list.on(ENTRY_ADDED_EVENT, add_callback)
        self.list.on(ENTRY_MOVED_EVENT, move_callback)
        self.list.on(ENTRY_REMOVED_EVENT, remove_callback)
        self.list.set_entries(['c', 'b', 'f'])

        add_callback.assert_called_once_with('f', 2)
        move_callback.assert_called_once_with('c', 0)
        remove_callback.assert_any_call('a', 0)
        remove_callback.assert_any_call('d', 3)
        remove_callback.assert_any_call('e', 4)
        self.assertEqual(remove_callback.call_count, 3)
