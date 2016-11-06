from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy import client
from deepstreampy.record import Record
from deepstreampy.constants import connection_state
from deepstreampy.utils import Undefined
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
        self.record = Record('testRecord',
                             {},
                             self.connection,
                             self.options,
                             self.client)
        message = {'topic': 'R', 'action': 'R', 'data': ['testRecord', 0, '{}']}
        self.record._on_message(message)

    def test_create_record(self):
        self.assertDictEqual(self.record.get(), {})
        self.iostream.write.assert_called_with(
            "R{0}CR{0}testRecord{1}".format(chr(31), chr(30)).encode())

    def test_send_update_message(self):
        self.record.set({'firstname': 'John'})
        expected = ("R{0}U{0}testRecord{0}1{0}{{\"firstname\":\"John\"}}{1}"
                    .format(chr(31), chr(30)).encode())
        self.iostream.write.assert_called_with(expected)
        self.assertDictEqual(self.record.get(), {'firstname': 'John'})
        self.assertEquals(self.record.version, 1)

    def test_send_patch_message(self):
        self.record.set('Smith', 'lastname')
        expected = ("R{0}P{0}testRecord{0}1{0}lastname{0}SSmith{1}"
                    .format(chr(31), chr(30)).encode())
        self.iostream.write.assert_called_with(expected)

    def test_delete_value(self):
        self.record.set({'firstname': 'John', 'lastname': 'Smith'})
        self.record.set(Undefined, 'lastname')
        self.assertDictEqual(self.record.get(), {'firstname': 'John'})

    def test_invalid(self):
        self.assertRaises(ValueError, self.record.set, Undefined)

    def tearDown(self):
        super(RecordTest, self).tearDown()
        self.iostream.mock_reset()

if __name__ == '__main__':
    unittest.main()
