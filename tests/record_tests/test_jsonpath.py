from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy import jsonpath
import unittest


class TestGet(unittest.TestCase):

    def setUp(self):
        self.test_data = {'firstname': 'John',
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
        value = jsonpath.get(self.test_data, 'firstname', False)
        self.assertEquals(value, 'John')

        value = jsonpath.get(self.test_data, 'lastname', False)
        self.assertEquals(value, 'Smith')

    def test_nested_path(self):
        value = jsonpath.get(self.test_data, 'address.street', False)
        self.assertEquals(value, 'currentStreet')

    def test_other_array_entries(self):
        value = jsonpath.get(self.test_data, 'pastAddresses[0]', False)
        self.assertEquals(value,
                          {'street': 'firststreet', 'postCode': 1001})

    def test_array_entries(self):
        value = jsonpath.get(self.test_data, 'pastAddresses[1]', False)
        self.assertEquals(value,
                          {'street': 'secondstreet', 'postCode': 2002})

    def test_values_from_array(self):
        value = jsonpath.get(self.test_data, 'pastAddresses[0].postCode', False)
        self.assertEquals(value, 1001)

    def test_handle_whitespace(self):
        value = jsonpath.get(self.test_data, 'pastAddresses[  1  ].postCode',
                             False)
        self.assertEquals(value, 2002)

    def test_handle_integers(self):
        value = jsonpath.get(self.test_data, 1234, False)
        self.assertEquals(value, 'integer index')

    def test_nonexisting_keys(self):
        value = jsonpath.get(self.test_data, 'does not exist', False)
        self.assertEqual(value, None)

    def test_nested_nonexisting(self):
        value = jsonpath.get(self.test_data, 'address.number', False)
        self.assertEqual(value, None)

    def test_nonexisting_array_index(self):
        value = jsonpath.get(self.test_data, 'pastAddresses[3]', False)
        self.assertEqual(value, None)

    def test_negative_array_index(self):
        value = jsonpath.get(self.test_data, 'pastAddresses[-1]', False)
        self.assertEquals(value,
                          {'street': 'secondstreet', 'postCode': 2002})

    def test_detect_changes(self):
        value = jsonpath.get(self.test_data, 'firstname', False)
        self.assertEquals(value, 'John')
        self.test_data['firstname'] = 'Adam'
        value = jsonpath.get(self.test_data, 'firstname', False)
        self.assertEquals(value, 'Adam')

    def test_detect_changes_to_arrays(self):
        value = jsonpath.get(self.test_data, 'pastAddresses[1].street', False)
        self.assertEquals(value, 'secondstreet')
        self.test_data['pastAddresses'].pop()
        value = jsonpath.get(self.test_data, 'pastAddresses[1].street', False)
        self.assertEquals(value, None)


class TestSet(unittest.TestCase):

    def setUp(self):
        self.test_data = {}

    def test_simple_value(self):
        jsonpath.set(self.test_data, 'firstname', 'John', False)
        self.assertEqual(self.test_data, {'firstname': 'John'})

    def test_nested_values(self):
        jsonpath.set(self.test_data, 'address.street', 'someStreet', False)
        self.assertEqual(self.test_data, {
            'address': {
                'street': 'someStreet'
            }
        })

    def test_arrays(self):
        jsonpath.set(self.test_data, 'pastAddresses[1].street', 'someStreet',
                     False)
        self.assertEquals(self.test_data, {
            'pastAddresses': [None, {'street': 'someStreet'}]
        })

    def test_extend_objects(self):
        self.test_data = {'firstname': 'John'}
        jsonpath.set(self.test_data, 'lastname', 'Smith', False)
        self.assertEqual(self.test_data, {'firstname': 'John',
                                          'lastname': 'Smith'})

    def test_extend_lists(self):
        self.test_data = {'firstname': 'John',
                          'animals': ['Bear', 'Cow', 'Ostrich']}
        jsonpath.set(self.test_data, 'animals[1]', 'Emu', False)
        self.assertEqual(self.test_data,
                         {'firstname': 'John',
                          'animals': ['Bear', 'Emu', 'Ostrich']})

if __name__ == '__main__':
    unittest.main()
