from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals


def remote_wins(record, remote_value, remote_version, callback):
    callback(None, remote_value)


def local_wins(record, remote_value, remote_version, callback):
    callback(None, record.get())
