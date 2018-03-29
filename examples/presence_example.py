"""
An example showing how to check who is logged into the deepstream.io server and
listen for changes.
"""
from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy import connect
from deepstreampy import presence

from tornado import gen
from tornado import ioloop

import signal


def listener(name, logged_in):
    if logged_in:
        print("{} logged in".format(name))
    else:
        print("{} logged out".format(name))

@gen.coroutine
def run():
    # We need (at least) two clients to demonstrate how presence works
    # Let's start with the first one
    client_A = yield connect("ws://localhost:6020/deepstream")
    yield client_A.login({"username": "A"})

    # Check who is currently logged in, expecting no one
    all_A = yield client_A.presence.get_all()
    print("Who does client A see? : {}".format(all_A))
    # Output: []

    # Subscribe to presence events, so we get notified every time someone logs
    # in or out of the server
    client_A.presence.subscribe(listener)

    # Create another client
    client_B = yield connect("ws://localhost:6020/deepstream")

    # When the second client logs in, the first client is notified thus
    # triggering the listener
    yield client_B.login({"username": "B"})

    # Using client B, when we check who is logged in we should get A
    all_B = yield client_B.presence.get_all()
    print("Who does client B see? : {}".format(all_B))
    # Output: ['A']


if __name__ == "__main__":
    run()
    signal.signal(signal.SIGINT, lambda _, __: ioloop.IOLoop.current().stop())
    ioloop.IOLoop.current().start()
