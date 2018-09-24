from __future__ import unicode_literals, print_function, division
import sys
import os
import time
import uuid
import zmq
import unittest
import xmlrunner

PY2 = sys.version_info.major == 2
if PY2:
    str = unicode

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

from zprocess.zlog.server import (
    ZMQLogServer,
    ERR_INVALID_COMMAND,
    ERR_WRONG_NUM_ARGS,
)

class Client(object):
    """Context manager for a mock client"""

    def __init__(self, testcase, port, filepath):
        context = zmq.Context.instance()
        self.sock = context.socket(zmq.DEALER)
        self.port = port
        self.filepath = filepath
        self.client_id = uuid.uuid4().hex.encode('utf8')
        self.testcase = testcase

    def __enter__(self):
        self.sock.connect('tcp://127.0.0.1:%d' % self.port)
        return self

    def send(self, msg, *args, **kwargs):
        return self.sock.send_multipart([b'', msg], *args, **kwargs)

    def send_multipart(self, msg, *args, **kwargs):
        return self.sock.send_multipart([b''] + msg, *args, **kwargs)

    def recv(self, *args, **kwargs):
        response = self.sock.recv_multipart(*args, **kwargs)
        self.testcase.assertEqual(response[0], b'')
        self.testcase.assertEqual(len(response), 2)
        return response[1]

    def recv_multipart(self, *args, **kwargs):
        response = self.sock.recv_multipart(*args, **kwargs)
        self.testcase.assertEqual(response[0], b'')
        return response[1:]

    def poll(self, *args, **kwargs):
        return self.sock.poll(*args, **kwargs)

    def assertReceived(self, response):
        self.testcase.assertEqual(self.recv(), response)

    def assertNoResponse(self):
        self.testcase.assertEqual(self.poll(100, zmq.POLLIN), 0)

    def __exit__(self, *args, **kwargs):
        self.sock.close()


class ZLogServerTests(unittest.TestCase):
    def setUp(self):
        # Run the server on a random port on localhost:
        self.server = ZMQLogServer(bind_address='tcp://127.0.0.1', silent=True)
        self.server.run_in_thread()
        self.port = self.server.port

    def tearDown(self):
        self.server.stop()
        self.server = None
        self.port = None

    def client(self, filepath):
        return Client(self, self.port, filepath)

    def test_hello(self):
        with self.client(None) as client:
            client.send(b'hello')
            client.assertReceived(b'hello')

    def test_protocol(self):
        with self.client(None) as client:
            client.send(b'protocol')
            client.assertReceived(b'1.0.0')

if __name__ == '__main__':
    output = 'test-reports'
    if PY2:
        output = output.encode('utf8')
    testRunner = xmlrunner.XMLTestRunner(output=output, verbosity=3)
    unittest.main(verbosity=3, testRunner=testRunner, exit=False)
