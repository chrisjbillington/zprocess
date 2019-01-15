from __future__ import unicode_literals, print_function, division
import sys
import os
import uuid
import shutil
import zmq
import unittest
import xmlrunner

PY2 = sys.version_info.major == 2
if PY2:
    str = unicode

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

BITBUCKET = os.getenv('CI', None) is not None

from zprocess.zlog.server import (
    ZMQLogServer,
    ERR_INVALID_COMMAND,
    ERR_WRONG_NUM_ARGS,
    ERR_BAD_ENCODING,
    RotatingFileHandler
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
        self.server = ZMQLogServer(
            bind_address='tcp://127.0.0.1',
            handler_class=RotatingFileHandler,
            handler_kwargs={'maxBytes': 256, 'backupCount': 3},
            silent=True,
        )
        self.server.run_in_thread()
        self.port = self.server.port

    def tearDown(self):
        self.server.stop()
        self.server = None
        self.port = None
        if os.path.exists('testdir'):
            if os.name == 'posix':
                os.system('chmod +w testdir')
            shutil.rmtree('testdir')

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

    def test_bad_request(self):
        with self.client(None) as client:
            # Bad command:
            client.send(b'not a command')
            client.assertReceived(ERR_INVALID_COMMAND)

            # Bad num args for check_access:
            client.send_multipart([b'check_access', b'1', b'2', b'3'])
            client.assertReceived(ERR_WRONG_NUM_ARGS)

            # Bad num args for done:
            client.send_multipart([b'done', b'1', b'2', b'3'])
            client.assertReceived(ERR_WRONG_NUM_ARGS)

            # Bad num args for log. No response, but we test the server doesn't crash.
            client.send_multipart([b'log', b'1', b'2'])
            # Still running?
            client.send(b'hello')
            client.assertReceived(b'hello')

            # Bad filepath (contains nulls):
            client.send_multipart([b'check_access', b'\x00'])
            client.assertReceived(ERR_BAD_ENCODING)

            # Bad filepath (not UTF8 encoded):
            client.send_multipart([b'check_access', b'\xff'])
            client.assertReceived(ERR_BAD_ENCODING)

    # The following two tests fail on Bitbucket pipelines, because for some reason
    # creating and accessing files always succeeds in the CI environment even if there
    # are insufficient permissions.
    @unittest.skipIf(BITBUCKET or os.name == 'nt', 'Skip on Windows and BitBucket')
    def test_no_access(self):
        with self.client(None) as client:
            client.send_multipart([b'check_access', b'/test.log'])
            self.assertIn(b'[Errno 13] Permission denied', client.recv())

    @unittest.skipIf(BITBUCKET or os.name == 'nt', 'Skip on Windows and BitBucket')
    def test_cant_create_files(self):
        os.mkdir('testdir')
        os.system('touch testdir/foo.log')
        os.system('chmod -w testdir')
        with self.client(None) as client:
            client.send_multipart([b'check_access', b'testdir/foo.log'])
            self.assertIn(b'[Errno 13] Permission denied', client.recv())


    def test_access(self):
        os.mkdir('testdir')
        with self.client(None) as client:
            client.send_multipart([b'check_access', b'testdir/foo.log'])
            client.assertReceived(b'ok')

    def test_log(self):
        os.mkdir('testdir')
        with self.client(None) as client:

            client.send_multipart([b'check_access', b'testdir/foo.log'])
            client.assertReceived(b'ok')

            msg = "test message"
            client.send_multipart(
                [b'log', client.client_id, b'testdir/foo.log', msg.encode('utf8')]
            )

            # Since things are asynchronous, we need to get a response to something to
            # know that our logging request was handled. Also, this will have the server
            # close the file so that its contents will be flushed.
            client.send_multipart([b'done', client.client_id, b'testdir/foo.log'])
            client.assertReceived(b'ok')

            with open('testdir/foo.log', 'r') as f:
                self.assertEqual(f.read(), msg + '\n')

if __name__ == '__main__':
    output = 'test-reports'
    if PY2:
        output = output.encode('utf8')
    testRunner = xmlrunner.XMLTestRunner(output=output, verbosity=3)
    unittest.main(verbosity=3, testRunner=testRunner, exit=not sys.flags.interactive)
