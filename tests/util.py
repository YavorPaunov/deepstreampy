from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from tornado import tcpserver, concurrent


def msg(value):
    return value.replace("|", chr(31)).replace("+", chr(30)).encode()


class FakeServer(tcpserver.TCPServer):

    def __init__(self):
        super(FakeServer, self).__init__()
        self.queued_messages = list()
        self.received_messages = list()
        self.stream = None
        self.connections = []
        self._messsage_future = None

    def write(self, message):
        self.queued_messages.append(message)
        self.stream.write(message.encode())

    def read(self, data):
        message = data.decode()
        self.received_messages.append(message)
        if self._messsage_future:
            self._messsage_future.set_result(message)

    def handle_stream(self, stream, address):
        self.connections.append(stream)
        self.stream = stream
        self.stream.read_until_close(None, self.read)

    def reset_message_count(self):
        self.received_messages = list()

    def await_message(self):
        self._messsage_future = concurrent.Future()
        return self._messsage_future
