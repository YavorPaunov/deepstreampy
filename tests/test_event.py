from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy.constants import connection_state

from deepstreampy import client
from tests.util import msg
import unittest
import sys

if sys.version_info[0] < 3:
    import mock
else:
    from unittest import mock

URL = "ws://localhost:7777/deepstream"


class EventsTest(unittest.TestCase):

    def setUp(self):
        super(EventsTest, self).setUp()

        self.client = client.Client(URL)
        self.iostream = mock.Mock()
        self.client._connection._state = connection_state.OPEN
        self.client._connection._stream = self.iostream
        self.connection = self.client._connection

        self.event_callback = mock.Mock()
        self.error_callback = mock.Mock()

    def test_handler(self):
        self.iostream.write_message.assert_not_called()
        self.client.event.emit('myEvent', 6)
        self.iostream.write_message.assert_called_with(
            msg('E|EVT|myEvent|N6+').encode())

        self.client.event.subscribe('myEvent', self.event_callback)
        self.iostream.write_message.assert_called_with(
            msg('E|S|myEvent+').encode())

        self.client.on('error', self.error_callback)
        # self.error_callback.assert_called_with(
        #    'E', 'ACK_TIMEOUT',  'No ACK message received in time for myEvent')
        self.event_callback.assert_not_called()
        self.client.event._handle({'topic': 'EVENT',
                                   'action': 'EVT',
                                   'data': ['myEvent', 'N23']})
        self.event_callback.assert_called_with(23)

        self.client.event._handle({'topic': 'EVENT',
                                   'action': 'EVT',
                                   'data': ['myEvent']})
        self.event_callback.assert_called_with()

        self.client.event._handle({'topic': 'EVENT',
                                   'action': 'EVT',
                                   'data': ['myEvent', 'notTypes']})
        self.error_callback.assert_called_with('UNKNOWN_TYPE (notTypes)',
                                               'MESSAGE_PARSE_ERROR',
                                               'X')
        self.event_callback.reset_mock()
        self.client.event.unsubscribe('myEvent', self.event_callback)
        self.client.event.emit('myEvent', 11)
        self.event_callback.assert_not_called()

        self.client.event._handle({'topic': 'EVENT',
                                   'action': 'L',
                                   'data': ['myEvent']})
        self.error_callback.assert_called_with('myEvent',
                                               'UNSOLICITED_MESSAGE',
                                               'E')

    def test_accept(self):
        def listen_callback(data, is_subscribed, response):
            response['accept']()

        self.client.event.listen('a/.*', listen_callback)
        self.client.event._handle({'topic': 'E',
                                   'action': 'SP',
                                   'data': ['a/.*', 'a/1']})

        self.iostream.write_message.assert_called_with(
            msg('E|LA|a/.*|a/1+').encode())

    def test_reject(self):
        def listen_callback(data, is_subscribed, response):
            response['reject']()

        self.client.event.listen('b/.*', listen_callback)
        self.client.event._handle({'topic': 'E',
                                   'action': 'SP',
                                   'data': ['b/.*', 'b/1']})

        self.iostream.write_message.assert_called_with(
            msg('E|LR|b/.*|b/1+').encode())

    def test_accept_and_discard(self):
        def listen_callback(data, is_subscribed, response=None):
            if is_subscribed:
                response['accept']()

                self.client.event._handle({'topic': 'E',
                                           'action': 'SR',
                                           'data': ['b/.*', 'b/2']})

        self.client.event.listen('b/.*', listen_callback)
        self.client.event._handle({'topic': 'E',
                                   'action': 'SP',
                                   'data': ['b/.*', 'b/2']})

        self.iostream.write_message.assert_called_with(
            msg('E|LA|b/.*|b/2+').encode())

    def test_accept_unlisten(self):

        def listen_callback(data, is_subscribed, response):
            response['accept']()

        self.client.event.listen('a/.*', listen_callback)
        self.client.event._handle({'topic': 'E',
                                   'action': 'SP',
                                   'data': ['a/.*', 'a/1']})

        self.iostream.write_message.assert_called_with(
            msg('E|LA|a/.*|a/1+').encode())

        self.client.event.unlisten('a/.*')
        self.iostream.write_message.assert_called_with(
            msg('E|UL|a/.*+').encode())

        self.iostream.reset_mock()
        self.client.event._handle({'topic': 'E',
                                   'action': 'A',
                                   'data': ['UL', 'a/.*']})
        self.client.on('error', self.error_callback)
        self.client.event._handle({'topic': 'E',
                                   'action': 'SP',
                                   'data': ['a/.*', 'a/1']})
        self.error_callback.assert_called_with('a/.*',
                                               'UNSOLICITED_MESSAGE',
                                               'E')
        self.iostream.write_message.assert_not_called()

        self.client.event.unlisten('a/.*')
        self.error_callback.assert_called_with('a/.*',
                                               'NOT_LISTENING',
                                               'X')
