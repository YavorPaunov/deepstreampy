from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy.client import Client


def connect(url, **options):
    c = Client(url, **options)
    yield c.connect()
