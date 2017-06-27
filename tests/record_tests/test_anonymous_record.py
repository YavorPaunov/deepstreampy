from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy import client
from deepstreampy.record import AnonymousRecord
from deepstreampy.constants import connection_state

from tornado import testing
from tornado.concurrent import Future

import sys

if sys.version_info[0] < 3:
    import mock
else:
    from unittest import mock

URL = "ws://localhost:7777/deepstream"


class AnonymousRecordTest(testing.AsyncTestCase):

    def setUp(self):
        super(AnonymousRecordTest, self).setUp()
        self.client = client.Client(URL)
        self.handler = mock.Mock()
        self.handler.stream.closed = mock.Mock(return_value=False)
        f = Future()
        f.set_result(None)
        self.handler.write_message = mock.Mock(return_value=f)

        self.client._connection._state = connection_state.OPEN
        self.client._connection._websocket_handler = self.handler
        self.connection = self.client._connection
        self.io_loop = self.connection._io_loop
        self.record_handler = self.client.record
        self.options = {}

        self.general_callback = mock.Mock()
        self.firstname_callback = mock.Mock()
        self.ready_callback = mock.Mock()
        self.name_changed_callback = mock.Mock()
        self.error_callback = mock.Mock()

    @testing.gen_test
    def test_anonymous_record(self):
        # Creates the anonymous record
        anon_record = yield self.record_handler.get_anonymous_record()
        self.assertIsNone(anon_record.get())
        self.assertIsNone(anon_record.name)

        # Works before name is set
        with self.assertRaises(AttributeError):
            anon_record.set()

        anon_record.subscribe(self.general_callback)
        anon_record.subscribe(self.firstname_callback, "firstname")
        anon_record.on("ready", self.ready_callback)

        self.firstname_callback.assert_not_called()
        self.general_callback.assert_not_called()
        self.handler.write_message.assert_not_called()

        # Request a record when name is set
        anon_record.name = "recordA"
        self.assertEquals(anon_record.name, "recordA")
        self.handler.write_message.assert_called_with(
            "R{0}CR{0}recordA{1}".format(chr(31), chr(30)).encode())

        # Updates subscription once the record is ready
        self.firstname_callback.assert_not_called()
        self.general_callback.assert_not_called()
        self.ready_callback.assert_not_called()

        self.record_handler.handle(
            {"topic": "R",
             "action": "R",
             "data": ["recordA", 1, '{"firstname":"Yavor"}']})

        self.assertEquals(self.ready_callback.call_count, 1)
        self.firstname_callback.assert_called_once_with("Yavor")
        self.general_callback.assert_called_once_with({"firstname": "Yavor"})

        # Does nothing when another record changes
        yield self.record_handler.get_record("recordB")

        self.record_handler.handle(
            {"topic": "R",
             "action": "R",
             "data": [
                 "recordB", 1, '{"firstname":"John", "lastname":"Smith"}']})

        self.assertEquals(self.ready_callback.call_count, 1)
        self.firstname_callback.assert_called_with("Yavor")
        self.general_callback.assert_called_with({"firstname": "Yavor"})

        # Updates subscriptions when the record changes to an existing one
        anon_record.name = "recordB"
        self.assertEquals(self.ready_callback.call_count, 2)
        self.firstname_callback.assert_called_with("John")
        self.general_callback.assert_called_with({"firstname": "John",
                                                  "lastname": "Smith"})

        # Proxies calls through the underlying record
        recordB = yield self.record_handler.get_record("recordB")
        self.assertEquals(recordB.get("lastname"), "Smith")
        anon_record.set("Doe", "lastname")
        self.assertEquals(recordB.get("lastname"), "Doe")

        # No error thrown if record is reset after being destroyed
        anon_record._record.on("error", self.error_callback)
        self.record_handler.handle(
            {"topic": "R",
             "action": "A",
             "data": ["D", "recordB", 1]})

        self.assertEquals(self.ready_callback.call_count, 2)
        self.error_callback.assert_not_called()

        # Emits nameChanged when name is changed
        anon_record.on("nameChanged", self.name_changed_callback)
        anon_record.name = "recordC"

        self.name_changed_callback.assert_called_once_with("recordC")

        # Emits an additional ready event once the new record becomes available
        self.assertEquals(self.ready_callback.call_count, 2)

        self.record_handler.handle(
            {"topic": "R",
             "action": "R",
             "data": [
                 "recordC", 1, '{"firstname":"Yavor","lastname":"Paunov"}']})
        self.assertEquals(self.ready_callback.call_count, 3)
