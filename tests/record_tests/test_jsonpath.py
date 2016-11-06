from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy.record import JSONPath
import unittest
import sys

if sys.version_info[0] < 3:
    import mock
else:
    from unittest import mock


class TestGet(unittest.TestCase):

    def setUp(self):
        self.test_record = mock.Mock()
        self.test_record._data = {'firstname': 'John',
                                  'lastname': 'Smith',
                                  'address': {
                                      'street': 'currentStreet'
                                  },
                                  'pastAddresses': [
                                    {'street': 'firststreet', 'postCode': 1001},
                                    {'street': 'secondstreet', 'postCode': 2002}
                                  ],
                                  1234: 'integer index'}

    def test_simple_path(self):
        json_path = JSONPath(self.test_record, 'firstname')
        self.assertEquals(json_path.get_value(), 'John')

        json_path = JSONPath(self.test_record, 'lastname')
        self.assertEquals(json_path.get_value(), 'Smith')

    def test_nested_path(self):
        json_path = JSONPath(self.test_record, 'address.street')
        self.assertEquals(json_path.get_value(), 'currentStreet')

    def test_other_array_entries(self):
        json_path = JSONPath(self.test_record, 'pastAddresses[0]')
        self.assertEquals(json_path.get_value(),
                          {'street': 'firststreet', 'postCode': 1001})

    def test_array_entries(self):
        json_path = JSONPath(self.test_record, 'pastAddresses[1]')
        self.assertEquals(json_path.get_value(),
                          {'street': 'secondstreet', 'postCode': 2002})

    def test_values_from_array(self):
        json_path = JSONPath(self.test_record, 'pastAddresses[0].postCode')
        self.assertEquals(json_path.get_value(), 1001)

    def test_handle_whitespace(self):
        json_path = JSONPath(self.test_record, 'pastAddresses[  1  ].postCode')
        self.assertEquals(json_path.get_value(), 2002)

    def test_handle_integers(self):
        json_path = JSONPath(self.test_record, 1234)
        self.assertEquals(json_path.get_value(), 'integer index')

    def test_nonexisting_keys(self):
        json_path = JSONPath(self.test_record, 'does not exist')
        self.assertIsNone(json_path.get_value())

    def test_nested_nonexisting(self):
        json_path = JSONPath(self.test_record, 'address.number')
        self.assertIsNone(json_path.get_value())

    def test_nonexisting_array_index(self):
        json_path = JSONPath(self.test_record, 'pastAddresses[3]')
        self.assertIsNone(json_path.get_value())

    def test_negative_array_index(self):
        json_path = JSONPath(self.test_record, 'pastAddresses[-1]')
        self.assertEquals(json_path.get_value(),
                          {'street': 'secondstreet', 'postCode': 2002})

    def test_detect_changes(self):
        json_path = JSONPath(self.test_record, 'firstname')
        self.assertEquals(json_path.get_value(), 'John')
        self.test_record._data['firstname'] = 'Adam'
        self.assertEquals(json_path.get_value(), 'Adam')

    def test_detect_changes_to_arrays(self):
        json_path = JSONPath(self.test_record, 'pastAddresses[1].street')
        self.assertEquals(json_path.get_value(), 'secondstreet')
        self.test_record._data['pastAddresses'].pop()
        self.assertEquals(json_path.get_value(), None)


class TestSet(unittest.TestCase):

    def setUp(self):
        self.record = mock.Mock()
        self.record._data = {}

    def test_simple_value(self):
        json_path = JSONPath(self.record, 'firstname')
        json_path.set_value('John')
        self.assertEquals(json_path.get_value(), 'John')
        self.assertDictEqual(self.record._data, {'firstname': 'John'})

    def test_nested_values(self):
        json_path = JSONPath(self.record, 'address.street')
        json_path.set_value('someStreet')
        self.assertEquals(json_path.get_value(), 'someStreet')
        self.assertDictEqual(self.record._data, {
            'address': {
                'street': 'someStreet'
            }
        })

    def test_arrays(self):
        json_path = JSONPath(self.record, 'pastAddresses[1].street')
        json_path.set_value('someStreet')
        self.assertEquals(json_path.get_value(), 'someStreet')

    def test_extend_objects(self):
        self.record._data = {'firstname': 'John'}
        json_path = JSONPath(self.record, 'lastname')
        json_path.set_value('Smith')
        self.assertEquals(json_path.get_value(), 'Smith')
        self.assertDictEqual(self.record._data, {'firstname': 'John',
                                                 'lastname': 'Smith'})

    def test_extend_lists(self):
        self.record._data = {'firstname': 'John',
                             'animals': ['Bear', 'Cow', 'Ostrich']}
        json_path = JSONPath(self.record, 'animals[1]')
        json_path.set_value('Emu')
        self.assertEquals(json_path.get_value(), 'Emu')
        self.assertDictEqual(self.record._data,
                             {'firstname': 'John',
                              'animals': ['Bear', 'Emu', 'Ostrich']})

if __name__ == '__main__':
    unittest.main()
