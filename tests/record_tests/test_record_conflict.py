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


class UpdateNotInSync(unittest.TestCase):

    def setUp(self):
        self.error_callback = mock.Mock()
        self.subscribe_callback = mock.Mock()

        self.client = client.Client(URL)
        self.handler = mock.Mock()
        self.handler.stream.closed = mock.Mock(return_value=False)
        self.client._connection._state = connection_state.OPEN
        self.client._connection._websocket_handler = self.handler

        self.record = self._create_record()
        self.error_callback.reset_mock()
        self.subscribe_callback.reset_mock()

    def _create_record(self):
        options = {
            'merge_strategy': merge_strategies.remote_wins
        }

        record = Record('recordConflict', {}, self.client._connection, options,
                        self.client)
        record.on('error', self.error_callback)
        record.subscribe(self.subscribe_callback)
        record._on_message(
            {'topic': 'R', 'action': 'R', 'data': ['recordConflict', 3, '{}']})

        return record

    def test_ahead_of_local_with_diff(self):
        self.record._on_message(
            {'topic': 'R', 'action': 'U', 'data': [
                'recordConflict', 5, '{ "reason": "skippedVersion"}']})
        self.error_callback.assert_not_called()
        self.subscribe_callback.assert_called_with({"reason": "skippedVersion"})
        self.assertEqual(self.record.version, 5)

    def test_behind_local_with_diff(self):
        self.record._on_message(
            {'topic': 'R', 'action': 'U', 'data': [
                'recordConflict', 2, '{ "otherReason": "behindVersion"}']})

        self.error_callback.assert_not_called()
        self.subscribe_callback.assert_called_with(
            {"otherReason": "behindVersion"})
        self.assertEqual(self.record.version, 2)

    def test_ahead_of_local(self):
        self.record._on_message(
            {'topic': 'R', 'action': 'U', 'data': [
                'recordConflict', 5, '{}']})
        self.error_callback.assert_not_called()
        self.subscribe_callback.assert_not_called()
        self.assertEqual(self.record.version, 5)

    def test_behind_local(self):
        self.record._on_message(
            {'topic': 'R', 'action': 'U', 'data': [
                'recordConflict', 2, '{}']})
        self.error_callback.assert_not_called()
        self.subscribe_callback.assert_not_called()
        self.assertEqual(self.record.version, 2)

    def test_merge_failure(self):
        def failing_merge_strategy(
                record, remote_value, remote_version, callback):
            callback("error while merging", remote_value)

        self.record.merge_strategy = failing_merge_strategy
        self.record._on_message(
            {'topic': 'R', 'action': 'U', 'data': [
                'recordConflict', 2, '{}']})

        self.error_callback.assert_called_once_with(
            'VERSION_EXISTS', 'received update for 2 but version is 3')


class PatchNotInSync(unittest.TestCase):

    def setUp(self):
        self.error_callback = mock.Mock()
        self.subscribe_callback = mock.Mock()

        self.client = client.Client(URL)
        self.handler = mock.Mock()
        self.handler.stream.closed = mock.Mock(return_value=False)
        self.client._connection._state = connection_state.OPEN
        self.client._connection._websocket_handler = self.handler

        self.record = self._create_record()

        self.record._on_message(
            {'topic': 'R',
             'action': 'P',
             'data': ['recordConflict', 5, 'b.b1', 'SanotherValue']})

        self.handler.write_message.assert_called_with(
            msg('R|SN|recordConflict+'))

        self.error_callback.reset_mock()
        self.subscribe_callback.reset_mock()

    def _create_record(self):
        options = {
            'merge_strategy': merge_strategies.remote_wins
        }

        record = Record('recordConflict', {}, self.client._connection, options,
                        self.client)
        record.on('error', self.error_callback)
        record.subscribe(self.subscribe_callback)
        record._on_message(
            {'topic': 'R', 'action': 'R',
             'data': ['recordConflict', 8,
                      '{ "a": "a", "b": { "b1": "b1" }, "c": "c" }']})

        return record

    def test_ahead_of_local_with_diff(self):
        self.record._on_message(
            {'topic': 'R',
             'action': 'P',
             'data': ['recordConflict', 15, 'b.b1', 'SanotherValue']})

        self.record._on_message(
            {'topic': 'R',
             'action': 'R',
             'data': [
                 'recordConflict',
                 15,
                 '{ "a": "a", "b": { "b1" : "anotherValue" }, "c": "c" }']})

        self.error_callback.assert_not_called()
        self.subscribe_callback.assert_called_with({"a": "a",
                                                    "b": {"b1": "anotherValue"},
                                                    "c": "c"})

        self.handler.write_message.assert_called_with(
            msg('R|SN|recordConflict+'))

        self.assertEqual(self.record.version, 15)

    def test_behind_local_with_diff(self):
        self.record._on_message(
            {'topic': 'R',
             'action': 'P',
             'data': ['recordConflict', 2, 'b.b1', 'SanotherValue']})

        self.record._on_message(
            {'topic': 'R',
             'action': 'R',
             'data': [
                 'recordConflict',
                 2,
                 '{ "a": "a", "b": { "b1" : "anotherValue" }, "c": "c" }']})

        self.error_callback.assert_not_called()
        self.subscribe_callback.assert_called_with({"a": "a",
                                                    "b": {"b1": "anotherValue"},
                                                    "c": "c"})

        self.handler.write_message.assert_called_with(
            msg('R|SN|recordConflict+'))

        self.assertEqual(self.record.version, 2)

    def test_ahead_of_local(self):
        self.record._on_message(
            {'topic': 'R',
             'action': 'P',
             'data': ['recordConflict', 15, 'b.b1', 'Sb1']})

        self.record._on_message(
            {'topic': 'R',
             'action': 'R',
             'data': [
                 'recordConflict',
                 15,
                 '{ "a": "a", "b": { "b1" : "b1" }, "c": "c" }']})

        self.error_callback.assert_not_called()
        self.subscribe_callback.assert_not_called()

        self.handler.write_message.assert_called_with(
            msg('R|SN|recordConflict+'))

        self.assertEqual(self.record.version, 15)

    def test_behind_local(self):
        self.record._on_message(
            {'topic': 'R',
             'action': 'P',
             'data': ['recordConflict', 2, 'b.b1', 'Sb1']})

        self.record._on_message(
            {'topic': 'R',
             'action': 'R',
             'data': [
                 'recordConflict',
                 2,
                 '{ "a": "a", "b": { "b1" : "b1" }, "c": "c" }']})

        self.error_callback.assert_not_called()
        self.subscribe_callback.assert_not_called()

        self.handler.write_message.assert_called_with(
            msg('R|SN|recordConflict+'))

        self.assertEqual(self.record.version, 2)

    def test_merge_failure(self):
        def failing_merge_strategy(
                record, remote_value, remote_version, callback):
            callback("error while merging", remote_value)

        self.record.merge_strategy = failing_merge_strategy

        self.record._on_message(
            {'topic': 'R',
             'action': 'P',
             'data': ['recordConflict', 2, 'b.b1', 'SanotherValue']})

        self.record._on_message(
            {'topic': 'R',
             'action': 'R',
             'data': [
                 'recordConflict',
                 2,
                 '{ "a": "a", "b": { "b1" : "b1" }, "c": "c" }']})

        self.error_callback.assert_called_once_with(
            'VERSION_EXISTS', 'received update for 2 but version is 8')


class VersionExists(unittest.TestCase):

    def setUp(self):
        self.error_callback = mock.Mock()
        self.subscribe_callback = mock.Mock()

        self.client = client.Client(URL)
        self.handler = mock.Mock()
        self.handler.stream.closed = mock.Mock(return_value=False)
        self.client._connection._state = connection_state.OPEN
        self.client._connection._websocket_handler = self.handler

        self.record = self._create_record()

        self.record._on_message(
            {'topic': 'R',
             'action': 'P',
             'data': ['recordConflict', 5, 'b.b1', 'SanotherValue']})

        self.handler.write_message.assert_called_with(
            msg('R|SN|recordConflict+'))

        self.error_callback.reset_mock()
        self.subscribe_callback.reset_mock()

    def _create_record(self):
        options = {
            'merge_strategy': merge_strategies.remote_wins
        }

        record = Record('recordConflict', {}, self.client._connection, options,
                        self.client)
        record.on('error', self.error_callback)
        record.subscribe(self.subscribe_callback)
        record._on_message(
            {'topic': 'R', 'action': 'R', 'data': ['recordConflict', 3, '{}']})

        return record

    def test_ahead_of_local_with_diff(self):
        self.record._on_message(
            {'topic': 'R', 'action': 'E', 'data': [
                'VERSION_EXISTS',
                'recordConflict', 5,
                '{ "reason": "skippedVersion"}']})
        self.error_callback.assert_not_called()
        self.subscribe_callback.assert_called_with({"reason": "skippedVersion"})
        self.assertEqual(self.record.version, 5)

    def test_behind_local_with_diff(self):
        self.record._on_message(
            {'topic': 'R', 'action': 'E', 'data': [
                'VERSION_EXISTS',
                'recordConflict', 2,
                '{ "reason": "behindVersion"}']})
        self.error_callback.assert_not_called()
        self.subscribe_callback.assert_called_with({"reason": "behindVersion"})
        self.assertEqual(self.record.version, 2)

    def test_ahead_of_local(self):
        self.record._on_message(
            {'topic': 'R', 'action': 'E', 'data': [
                'VERSION_EXISTS',
                'recordConflict', 5,
                '{}']})
        self.error_callback.assert_not_called()
        self.subscribe_callback.assert_not_called()
        self.assertEqual(self.record.version, 5)

    def test_behind_local(self):
        self.record._on_message(
            {'topic': 'R', 'action': 'E', 'data': [
                'VERSION_EXISTS',
                'recordConflict', 2,
                '{}']})
        self.error_callback.assert_not_called()
        self.subscribe_callback.assert_not_called()
        self.assertEqual(self.record.version, 2)


class WriteAcknowledgement(unittest.TestCase):

    def setUp(self):

        self.error_callback = mock.Mock()
        self.subscribe_callback = mock.Mock()
        self.set_callback = mock.Mock()

        self.client = client.Client(URL)
        self.handler = mock.Mock()
        self.handler.stream.closed = mock.Mock(return_value=False)
        self.client._connection._state = connection_state.OPEN
        self.client._connection._websocket_handler = self.handler

        self.record = self._create_record()

        self.error_callback.reset_mock()
        self.subscribe_callback.reset_mock()
        self.set_callback.reset_mock()

    def _create_record(self):
        options = {
            'merge_strategy': merge_strategies.local_wins
        }

        record = Record('recordConflict', {}, self.client._connection, options,
                        self.client)
        record.on('error', self.error_callback)
        record.subscribe(self.subscribe_callback)
        record._on_message(
            {'topic': 'R', 'action': 'R',
             'data': ['recordConflict', 8,
                      '{ "a": "a", "b": { "b1": "b1" }, "c": "c" }']})

        return record

    def test_behind_with_diff(self):
        self.record.set({"name": "Alex"}, callback=self.set_callback)

        self.record._on_message(
            {'topic': 'R', 'action': 'E',
             'data': ['VERSION_EXISTS', 'recordConflict', 2,
                      '{"reason":"behindVersion"}', '{"writeSuccess": true}']})

        self.handler.write_message.assert_called_with(
            msg('R|U|recordConflict|3|{"name":"Alex"}|{"writeSuccess": true}+'))

        self.assertEqual(self.record.version, 3)

        self.record._on_message(
            {'topic': 'R', 'action': 'WA', 'data': [
                'conflictRecord', '[3]', 'L']})

        self.set_callback.assert_called_once_with(None)
