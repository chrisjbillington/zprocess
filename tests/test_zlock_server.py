from __future__ import unicode_literals, print_function, division
import sys
import os

import zmq
import unittest
import xmlrunner

PY2 = sys.version_info.major == 2
if PY2:
    str = unicode

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

from zprocess.locking.server import ZMQLockServer


class TemporaryREQSocket(object):
    """Context manager for a REQ socket connecting to a certain port on localhost"""

    def __init__(self, port):
        context = zmq.Context.instance()
        self.sock = context.socket(zmq.REQ)
        self.port = port

    def __enter__(self):
        self.sock.connect('tcp://127.0.0.1:%d' % self.port)
        return self.sock

    def __exit__(self, *args, **kwargs):
        self.sock.close()


class ZLockServerTests(unittest.TestCase):
    def setUp(self):
        # Run the server on a random port on localhost:
        self.server = ZMQLockServer(bind_address='tcp://127.0.0.1')
        self.server.run_in_thread()
        self.port = self.server.port

    def tearDown(self):
        self.server.stop()
        self.server = None
        self.port = None

    def test_hello(self):
        with TemporaryREQSocket(self.port) as sock:
            sock.send(b'hello')
            self.assertEqual(sock.recv(), b'hello')

    def test_uncontested_reader(self):
        with TemporaryREQSocket(self.port) as sock:
            # Reader acquires the lock:
            sock.send_multipart(
                [b'acquire', b'key_foo', b'client_foo', b'30', b'read_only']
            )
            self.assertEqual(sock.recv(), b'ok')
            # Reader releases the lock:
            sock.send_multipart([b'release', b'key_foo', b'client_foo'])
            self.assertEqual(sock.recv(), b'ok')

    def test_uncontested_writer(self):
        with TemporaryREQSocket(self.port) as sock:
            # Writer acquires the lock:
            sock.send_multipart([b'acquire', b'key_foo', b'client_foo', b'30'])
            self.assertEqual(sock.recv(), b'ok')
            # Writer releases the lock:
            sock.send_multipart([b'release', b'key_foo', b'client_foo'])
            self.assertEqual(sock.recv(), b'ok')

    def test_multiple_readers(self):
        with TemporaryREQSocket(self.port) as sock:
            # Reader acquires the lock:
            sock.send_multipart(
                [b'acquire', b'key_foo', b'client_foo', b'30', b'read_only']
            )
            self.assertEqual(sock.recv(), b'ok')
            # Another reader acquires the lock:
            sock.send_multipart(
                [b'acquire', b'key_foo', b'client_bar', b'30', b'read_only']
            )
            self.assertEqual(sock.recv(), b'ok')
            # Both release the lock:
            sock.send_multipart([b'release', b'key_foo', b'client_foo'])
            self.assertEqual(sock.recv(), b'ok')
            sock.send_multipart([b'release', b'key_foo', b'client_bar'])
            self.assertEqual(sock.recv(), b'ok')

    def test_reader_then_writer(self):
        sock1 = TemporaryREQSocket(self.port)
        sock2 = TemporaryREQSocket(self.port)
        with sock1 as reader, sock2 as writer:
            # Reader acquires the lock:
            reader.send_multipart(
                [b'acquire', b'key_foo', b'client_foo', b'30', b'read_only']
            )
            self.assertEqual(reader.recv(), b'ok')
            # Writer is blocked trying to acquire the lock:
            writer.send_multipart([b'acquire', b'key_foo', b'client_bar', b'30'])
            self.assertEqual(writer.poll(100, zmq.POLLIN), 0)
            # Reader releases the lock:
            reader.send_multipart([b'release', b'key_foo', b'client_foo'])
            self.assertEqual(reader.recv(), b'ok')
            # Writer should now get the lock:
            self.assertEqual(writer.recv(), b'ok')
            # Writer releases the lock:
            writer.send_multipart([b'release', b'key_foo', b'client_bar'])
            self.assertEqual(writer.recv(), b'ok')

    def test_waiting_writer_blocks_new_readers(self):
        sock1 = TemporaryREQSocket(self.port)
        sock2 = TemporaryREQSocket(self.port)
        sock3 = TemporaryREQSocket(self.port)
        with sock1 as reader1, sock2 as reader2, sock3 as writer:
            # Reader acquires the lock:
            reader1.send_multipart(
                [b'acquire', b'key_foo', b'reader1', b'30', b'read_only']
            )
            self.assertEqual(reader1.recv(), b'ok')
            # Writer is blocked trying to acquire the lock:
            writer.send_multipart([b'acquire', b'key_foo', b'writer', b'30'])
            self.assertEqual(writer.poll(100, zmq.POLLIN), 0)
            # Another reader attempts to acquire the lock but is blocked:
            reader2.send_multipart(
                [b'acquire', b'key_foo', b'reader2', b'30', b'read_only']
            )
            self.assertEqual(reader2.poll(100, zmq.POLLIN), 0)
            # The first reader releases the lock:
            reader1.send_multipart([b'release', b'key_foo', b'reader1'])
            self.assertEqual(reader1.recv(), b'ok')
            # The writer should get the lock now:
            self.assertEqual(writer.recv(), b'ok')
            # Not the other reader though:
            self.assertEqual(reader2.poll(100, zmq.POLLIN), 0)
            # The writer releases the lock:
            writer.send_multipart([b'release', b'key_foo', b'writer'])
            self.assertEqual(writer.recv(), b'ok')
            # Now the second reader should get the lock:
            self.assertEqual(reader2.recv(), b'ok')
            # Which it then releases:
            reader2.send_multipart([b'release', b'key_foo', b'reader2'])
            self.assertEqual(reader2.recv(), b'ok')

    def test_concurrent_error(self):
        sock1 = TemporaryREQSocket(self.port)
        sock2 = TemporaryREQSocket(self.port)
        sock3 = TemporaryREQSocket(self.port)
        with sock1 as writer1, sock2 as writer2, sock3 as writer2_copy:
            # Writer acquires the lock
            writer1.send_multipart([b'acquire', b'key_foo', b'writer1', b'30'])
            self.assertEqual(writer1.recv(), b'ok')
            # Another writer is blocked trying to acquire:
            writer2.send_multipart([b'acquire', b'key_foo', b'writer2', b'30'])
            self.assertEqual(writer2.poll(100, zmq.POLLIN), 0)
            # The second writer doesn't wait for a reply before retrying:
            writer2_copy.send_multipart([b'acquire', b'key_foo', b'writer2', b'30'])
            # It gets an error:
            self.assertEqual(
                writer2_copy.recv(),
                b'error: multiple concurrent requests with same client_id',
            )
            # The first writer releases the lock:
            writer1.send_multipart([b'release', b'key_foo', b'writer1'])
            self.assertEqual(writer1.recv(), b'ok')
            # The second writer gets it now:
            self.assertEqual(writer2.recv(), b'ok')
            # And releases it:
            writer2.send_multipart([b'release', b'key_foo', b'writer2'])
            self.assertEqual(writer2.recv(), b'ok')


if __name__ == '__main__':
    output = 'test-reports'
    if PY2:
        output = output.encode('utf8')
    testRunner = xmlrunner.XMLTestRunner(output=output, verbosity=3)
    unittest.main(verbosity=3, testRunner=testRunner, exit=False)
