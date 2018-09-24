from __future__ import unicode_literals, print_function, division
import sys
import os
import time
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
    # Task,
    # TaskQueue,
    # Lock,
    # NotHeld,
    # AlreadyWaiting,
    # InvalidReentry,
    # ERR_NOT_HELD,
    # ERR_INVALID_REENTRY,
    # ERR_CONCURRENT,
    # ERR_INVALID_COMMAND,
    # ERR_WRONG_NUM_ARGS,
    # ERR_TIMEOUT_INVALID,
    # ERR_READ_ONLY_WRONG,
)

class Client(object):
    """Context manager for a mock client"""

    def __init__(self, testcase, port, filepath):
        context = zmq.Context.instance()
        self.sock = context.socket(zmq.REQ)
        self.port = port
        self.key = filepath
        self.client_id = os.urandom(32)
        self.testcase = testcase

    def __enter__(self):
        self.sock.connect('tcp://127.0.0.1:%d' % self.port)
        return self

    def send(self, *args, **kwargs):
        return self.sock.send(*args, **kwargs)

    def send_multipart(self, *args, **kwargs):
        return self.sock.send_multipart(*args, **kwargs)

    def recv(self, *args, **kwargs):
        return self.sock.recv(*args, **kwargs)

    def recv_multipart(self, *args, **kwargs):
        return self.sock.recv_multipart(*args, **kwargs)

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


if __name__ == '__main__':
    output = 'test-reports'
    if PY2:
        output = output.encode('utf8')
    testRunner = xmlrunner.XMLTestRunner(output=output, verbosity=3)
    unittest.main(verbosity=3, testRunner=testRunner, exit=False)
