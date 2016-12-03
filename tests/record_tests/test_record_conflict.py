from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy import client
from deepstreampy.record import Record
from deepstreampy.constants import connection_state, merge_strategies
from tests.util import msg

import unittest
import sys

if sys.version_info[0] < 3:
    import mock
else:
    from unittest import mock

URL = "ws://localhost:7777/deepstream"


class TestMergeConflict(unittest.TestCase):

    def setUp(self):
        options = {
            'merge_strategy': merge_strategies.remote_wins
        }
        self.client = client.Client(URL)
        self.iostream = mock.Mock()
        self.client._connection._state = connection_state.OPEN
        self.client._connection._stream = self.iostream
        self.client._io_loop = mock.Mock()
        self.record = Record('someRecord', {}, self.client._connection, options,
                             self.client)

        self.error_callback = mock.Mock()
        self.subscribe_callback = mock.Mock()

        self.record.on('error', self.error_callback)
        self.record.subscribe(self.subscribe_callback)

        message = {}
        message['topic'] = 'R'
        message['action'] = 'R'
        message['data'] = ['TEST', 0, '{}']
        self.record._on_message(message)

    def test_out_of_sync(self):
        message = {}
        message['topic'] = 'R'
        message['action'] = 'U'
        message['data'] = ['TEST', 0, '{}']

        self.record._on_message(message)

        message['data'] = ['TEST', 5, '{"reason":"skippedVersion"}']
        self.record._on_message(message)

        self.error_callback.assert_not_called()
        self.iostream.write_message.assert_called_with(
            msg('R|U|someRecord|6|{"reason":"skippedVersion"}+').encode())
        self.subscribe_callback.assert_called_with({'reason': 'skippedVersion'})
