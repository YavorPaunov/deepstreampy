from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy import client
from deepstreampy.record import RecordHandler
from deepstreampy.constants import connection_state
from tests.util import msg

import sys

from tornado import testing
from tornado.concurrent import Future

if sys.version_info[0] < 3:
    import mock
else:
    from unittest import mock

URL = "ws://localhost:7777/deepstream"


class RecordListening(testing.AsyncTestCase):

    def setUp(self):
        self.client = client.Client(URL)
        self.handler = mock.Mock()
        self.handler.stream.closed = mock.Mock(return_value=False)
        f = Future()
        f.set_result(None)

        self.handler.write_message = mock.Mock(return_value=f)
        self.client._connection._state = connection_state.OPEN
        self.client._connection._websocket_handler = self.handler

        self.io_loop = self.client._connection._io_loop

        self._record_handler = RecordHandler(
            self.client._connection, self.client)

    def _handle_response(self):
        pass

    @testing.gen_test
    def test_provider_accepts(self):
        a1 = yield self._record_handler.get_record('a/1')
        self.assertFalse(a1.has_provider)

        def _response_callback(data, is_subscribed, response=None):
            response.accept()
            self.handler.write_message.assert_called_with(
                msg('R|LA|a/.*|a/1+'))
            self._record_handler.handle({'topic': 'R',
                                         'action': 'SH',
                                         'data': ['a/1', 'T']})
            self.assertTrue(a1.has_provider)

        yield self._record_handler.listen('a/.*', _response_callback)

        def _provider_changed_callback(has_provider):
            self.assertTrue(has_provider)
            self.assertTrue(a1.has_provider)

        a1.on('hasProviderChanged', _provider_changed_callback)

        self._record_handler.handle({'topic': 'R',
                                     'action': 'SP',
                                     'data': ['a/.*', 'a/1']})

    @testing.gen_test
    def test_provider_rejects(self):
        b1 = yield self._record_handler.get_record('b/1')
        self.assertFalse(b1.has_provider)

        def _response_callback(data, is_subscribed, response=None):
            response.reject()
            self.handler.write_message.assert_called_with(
                msg('R|LR|b/.*|b/1+'))

        yield self._record_handler.listen('b/.*', _response_callback)

        def _provider_changed_callback(has_provider):
            self.assertFalse(has_provider)
            self.assertFalse(b1.has_provider)

        self._record_handler.handle({'topic': 'R',
                                     'action': 'SP',
                                     'data': ['b/.*', 'b/1']})

        b1.on('hasProviderChanged', _provider_changed_callback)
        self._record_handler.handle({'topic': 'R',
                                     'action': 'SH',
                                     'data': ['b/1', 'F']})

        self.assertFalse(b1.has_provider)

    @testing.gen_test
    def test_provider_accepts_and_discards(self):
        yield self._record_handler.get_record('b/2')

        def _response_callback(data, is_subscribed, response=None):
            if is_subscribed:
                response.accept()
                self.handler.write_message.assert_called_with(
                    msg('R|LA|b/.*|b/2+'))

                self._record_handler.handle({'topic': 'R',
                                             'action': 'SR',
                                             'data': ['b/.*', 'b/1']})

        yield self._record_handler.listen('b/.*', _response_callback)

        self._record_handler.handle({'topic': 'R',
                                     'action': 'SR',
                                     'data': ['b/.*', 'b/1']})

    @testing.gen_test
    def test_provider_accepts_and_rejects(self):
        c1 = yield self._record_handler.get_record('c/1')
        d1 = yield self._record_handler.get_record('d/1')

        def _c1_response_callback(data, is_subscribed, response=None):
            response.accept()
            self.handler.write_message.assert_called_with(msg('R|LA|c/.*|c/1+'))
            self._record_handler.handle({'topic': 'R',
                                         'action': 'SH',
                                         'data': ['c/1', 'T']})
            self.assertTrue(c1.has_provider)

        self._record_handler.listen('c/.*', _c1_response_callback)

        def _d1_response_callback(data, is_subscribed, response=None):
            response.reject()
            self.handler.write_message.assert_called_with(msg('R|LR|d/.*|d/1+'))
            self.assertFalse(d1.has_provider)
            self.assertTrue(c1.has_provider)

        self._record_handler.listen('d/.*', _d1_response_callback)

        def _provider_changed_callback(has_provider):
            self.assertTrue(has_provider)
            self.assertTrue(c1.has_provider)

        c1.on('hasProviderChanged', _provider_changed_callback)

        self._record_handler.handle({'topic': 'R',
                                     'action': 'SP',
                                     'data': ['c/.*', 'c/1']})

        self._record_handler.handle({'topic': 'R',
                                     'action': 'SP',
                                     'data': ['d/.*', 'd/1']})
