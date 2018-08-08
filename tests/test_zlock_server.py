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

from zprocess.locking.server import ZMQLockServer, Task, TaskQueue, Lock
import zprocess.locking.server


class monkeypatch(object):
    """Context manager to temporarily monkeypatch an object attribute with
    some mocked attribute"""

    def __init__(self, obj, name, mocked_attr):
        self.obj = obj
        self.name = name
        self.real_attr = getattr(obj, name)
        self.mocked_attr = mocked_attr

    def __enter__(self):
        setattr(self.obj, self.name, self.mocked_attr)

    def __exit__(self, *args):
        setattr(self.obj, self.name, self.real_attr)


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
                b'error: multiple concurrent requests with same key and client_id',
            )
            # The second client then calls release before waiting for a response:
            writer2_copy.send_multipart([b'release', b'key_foo', b'writer2'])
            # It gets an error:
            self.assertEqual(
                writer2_copy.recv(),
                b'error: multiple concurrent requests with same key and client_id',
            )

            # The first writer releases the lock:
            writer1.send_multipart([b'release', b'key_foo', b'writer1'])
            self.assertEqual(writer1.recv(), b'ok')
            # The second writer gets it now:
            self.assertEqual(writer2.recv(), b'ok')
            # And releases it:
            writer2.send_multipart([b'release', b'key_foo', b'writer2'])
            self.assertEqual(writer2.recv(), b'ok')

    def test_change_mind(self):
        sock1 = TemporaryREQSocket(self.port)
        sock2 = TemporaryREQSocket(self.port)
        with sock1 as reader, sock2 as mind_changer:
            # Reader acquires the lock
            reader.send_multipart(
                [b'acquire', b'key_foo', b'reader', b'30', b'read_only']
            )
            self.assertEqual(reader.recv(), b'ok')
            # Speed up the response:
            with monkeypatch(zprocess.locking.server, 'MAX_RESPONSE_TIME', 0.1):
                # Another writer tries to acquire but is told to retry
                mind_changer.send_multipart(
                    [b'acquire', b'key_foo', b'mind_changer', b'30']
                )
                self.assertEqual(mind_changer.recv(), b'retry')
            # It retries, but now it wants a read-only lock:
            mind_changer.send_multipart(
                [b'acquire', b'key_foo', b'mind_changer', b'30', b'read_only']
            )
            # It should get it, since the other client is a reader:
            self.assertEqual(mind_changer.recv(), b'ok')
            # Both clients release:
            mind_changer.send_multipart([b'release', b'key_foo', b'mind_changer'])
            self.assertEqual(mind_changer.recv(), b'ok')
            reader.send_multipart([b'release', b'key_foo', b'reader'])
            self.assertEqual(reader.recv(), b'ok')

    def test_absentee_acquire_release(self):
        sock1 = TemporaryREQSocket(self.port)
        sock2 = TemporaryREQSocket(self.port)
        with sock1 as writer1, sock2 as writer2:
            # Client acquires the lock:
            writer1.send_multipart([b'acquire', b'key_foo', b'writer1', b'30'])
            self.assertEqual(writer1.recv(), b'ok')
            # Speed up the response:
            with monkeypatch(zprocess.locking.server, 'MAX_RESPONSE_TIME', 0.1):
                # Another client tries to acquire but is told to retry
                writer2.send_multipart([b'acquire', b'key_foo', b'writer2', b'30'])
                self.assertEqual(writer2.recv(), b'retry')
            # First client releases the lock, now the second client has the lock in absentia:
            writer1.send_multipart([b'release', b'key_foo', b'writer1'])
            self.assertEqual(writer1.recv(), b'ok')
            # Second client calls release, even though it hasn't retried yet:
            writer2.send_multipart([b'release', b'key_foo', b'writer2'])
            # It should get an error, since it didn't follow protocol in finishing
            # acquiring the lock:
            self.assertEqual(writer2.recv(), b'error: lock not held')

    def test_absentee_acquire_timeout(self):
        with TemporaryREQSocket(self.port) as sock:
            # First client acquires the lock:
            sock.send_multipart([b'acquire', b'key_foo', b'client_foo', b'30'])
            self.assertEqual(sock.recv(), b'ok')
            # Speed up the response:
            with monkeypatch(zprocess.locking.server, 'MAX_RESPONSE_TIME', 0.1):
                # Second client tries to acquire the lock, but is told to retry:
                sock.send_multipart([b'acquire', b'key_foo', b'client_bar', b'30'])
            self.assertEqual(sock.recv(), b'retry')
            # Before the second client retries, the first client releases the lock.
            # The second client should now have the lock in absentia.
            with monkeypatch(zprocess.locking.server, 'MAX_ABSENT_TIME', 0.2):
                sock.send_multipart([b'release', b'key_foo', b'client_foo'])
                self.assertEqual(sock.recv(), b'ok')
            # Before the second client retries, the first client asks for the lock
            # again:
            sock.send_multipart([b'acquire', b'key_foo', b'client_foo', b'30'])
            # It doesn't work at first, because the second client has the lock:
            self.assertEqual(sock.poll(100, zmq.POLLIN), 0)
            # But since the second client is not retrying, its lock times out
            # and the first client gets it:
            self.assertEqual(sock.recv(), b'ok')
            # Then it releases it:
            sock.send_multipart([b'release', b'key_foo', b'client_foo'])
            self.assertEqual(sock.recv(), b'ok')

            # The second client lost its spot in the queue, but should be able to
            # try again to acquire and release the lock as normal:
            sock.send_multipart([b'acquire', b'key_foo', b'client_bar', b'30'])
            self.assertEqual(sock.recv(), b'ok')
            sock.send_multipart([b'release', b'key_foo', b'client_bar'])
            self.assertEqual(sock.recv(), b'ok')

    def test_timeout(self):
        with TemporaryREQSocket(self.port) as sock:
            # Client acquires the lock with a short timeout
            sock.send_multipart([b'acquire', b'key_foo', b'client_foo', b'0.1'])
            self.assertEqual(sock.recv(), b'ok')
            # Client waits longer than the timeout before releasing:
            time.sleep(0.2)
            sock.send_multipart([b'release', b'key_foo', b'client_foo'])
            self.assertEqual(sock.recv(), b'error: lock not held')

    def test_already_held(self):
        with TemporaryREQSocket(self.port) as sock:
            # Client acquires the lock:
            sock.send_multipart([b'acquire', b'key_foo', b'client_foo', b'30'])
            self.assertEqual(sock.recv(), b'ok')
            # Client makes a second acquisition request without releasing:
            sock.send_multipart([b'acquire', b'key_foo', b'client_foo', b'30'])
            self.assertEqual(sock.recv(), b'error: lock already held')

    def test_malformed_requests(self):
        with TemporaryREQSocket(self.port) as sock:
            # Invalid command:
            sock.send_multipart([b'frobulate', b'key_foo', b'client_foo', b'30'])
            self.assertEqual(sock.recv(), b'error: invalid command')
            # read_only wrong:
            sock.send_multipart(
                [b'acquire', b'key_foo', b'client_foo', b'30', b'not_going_to_write']
            )
            self.assertEqual(
                sock.recv(), b"error: expected 'read_only', got not_going_to_write"
            )
            # Too few args to acquire:
            sock.send_multipart([b'acquire'])
            self.assertEqual(sock.recv(), b'error: wrong number of arguments')
            # Too many args to acquire:
            sock.send_multipart([b'acquire', b'1', b'2', b'3', b'4', b'5'])
            self.assertEqual(sock.recv(), b'error: wrong number of arguments')
            # Timeout not a number:
            sock.send_multipart([b'acquire', b'key_foo', b'client_foo', b'fs'])
            self.assertEqual(sock.recv(), b'error: timeout fs not a valid number')
            # Timeout not a valid number:
            sock.send_multipart([b'acquire', b'key_foo', b'client_foo', b'-inf'])
            self.assertEqual(sock.recv(), b'error: timeout -inf not a valid number')
            # Too few args to release:
            sock.send_multipart([b'release'])
            self.assertEqual(sock.recv(), b'error: wrong number of arguments')
            # Too many args to release:
            sock.send_multipart([b'release', b'1', b'2', b'3', b'4', b'5'])
            self.assertEqual(sock.recv(), b'error: wrong number of arguments')

            # Have to use a DEALER to make a message missing the initial empty message
            context = zmq.Context.instance()
            dealer = context.socket(zmq.DEALER)
            dealer.connect('tcp://127.0.0.1:%d' % self.port)
            try:
                dealer.send(b'foo')
                # expect nothing back:
                self.assertEqual(dealer.poll(100, zmq.POLLIN), 0)
            finally:
                dealer.close()

    def test_absentee_acquire(self):
        with TemporaryREQSocket(self.port) as sock:
            # First client acquires the lock:
            sock.send_multipart([b'acquire', b'key_foo', b'client_foo', b'30'])
            self.assertEqual(sock.recv(), b'ok')
            # Speed up the response:
            with monkeypatch(zprocess.locking.server, 'MAX_RESPONSE_TIME', 0.1):
                # Second client tries to acquire the lock, but is told to retry:
                sock.send_multipart([b'acquire', b'key_foo', b'client_bar', b'30'])
            self.assertEqual(sock.recv(), b'retry')
            # Before the second client retries, the first client releases the lock:
            sock.send_multipart([b'release', b'key_foo', b'client_foo'])
            self.assertEqual(sock.recv(), b'ok')
            # The second client should receive the lock upon retrying:
            sock.send_multipart([b'acquire', b'key_foo', b'client_bar', b'30'])
            self.assertEqual(sock.recv(), b'ok')

    def test_retry(self):
        sock1 = TemporaryREQSocket(self.port)
        sock2 = TemporaryREQSocket(self.port)
        with sock1 as writer1, sock2 as writer2:
            # First client acquires the lock:
            writer1.send_multipart([b'acquire', b'key_foo', b'client_foo', b'30'])
            self.assertEqual(writer1.recv(), b'ok')
            # Speed up the response:
            with monkeypatch(zprocess.locking.server, 'MAX_RESPONSE_TIME', 0.1):
                # Second client tries to acquire the lock but is told to retry:
                writer2.send_multipart([b'acquire', b'key_foo', b'client_bar', b'30'])
                self.assertEqual(writer2.recv(), b'retry')
            # Second client retries:
            writer2.send_multipart([b'acquire', b'key_foo', b'client_bar', b'30'])
            # First client releases:
            writer1.send_multipart([b'release', b'key_foo', b'client_foo'])
            self.assertEqual(writer1.recv(), b'ok')
            # Second client gets it:
            self.assertEqual(writer2.recv(), b'ok')

    def test_absent_give_up(self):
        sock1 = TemporaryREQSocket(self.port)
        sock2 = TemporaryREQSocket(self.port)
        with sock1 as writer1, sock2 as writer2:
            for read_only in [True, False]:
                # First client acquires the lock:
                writer1.send_multipart([b'acquire', b'key_foo', b'client_foo', b'30'])
                self.assertEqual(writer1.recv(), b'ok')
                # Speed up the response and away timeout:
                with monkeypatch(zprocess.locking.server, 'MAX_RESPONSE_TIME', 0.1):
                    with monkeypatch(zprocess.locking.server, 'MAX_ABSENT_TIME', 0.1):
                        # Second client tries to acquire the lock but is told to retry:
                        request = [b'acquire', b'key_foo', b'client_bar', b'30']
                        if read_only:
                            request.append(b'read_only')
                        writer2.send_multipart(request)
                        self.assertEqual(writer2.recv(), b'retry')
                        # Second client waits too long and loses its spot in the queue
                        time.sleep(0.2)
                        # First client releases the lock:
                        writer1.send_multipart([b'release', b'key_foo', b'client_foo'])
                        self.assertEqual(writer1.recv(), b'ok')
                        # And then acquires it again, sucessfully since client 1 has
                        # given up:
                        writer1.send_multipart(
                            [b'acquire', b'key_foo', b'client_foo', b'30']
                        )
                        self.assertEqual(writer1.recv(), b'ok')
                        # Second client retries:
                        writer2.send_multipart(request)
                        # But is told to wait since it lost its spot in the queue:
                        self.assertEqual(writer2.recv(), b'retry')
                        # It immediately retries:
                        writer2.send_multipart(request)
                        # First client releases:
                        writer1.send_multipart([b'release', b'key_foo', b'client_foo'])
                        self.assertEqual(writer1.recv(), b'ok')
                        # Then second client gets the lock:
                        self.assertEqual(writer2.recv(), b'ok')
                        # And finally releases:
                        writer2.send_multipart([b'release', b'key_foo', b'client_bar'])
                        self.assertEqual(writer2.recv(), b'ok')

    def test_locks_independent(self):
        with TemporaryREQSocket(self.port) as sock:
            # Two clients acquiring different locks should be able to:
            sock.send_multipart([b'acquire', b'key_foo', b'client_foo', b'30'])
            self.assertEqual(sock.recv(), b'ok')
            sock.send_multipart([b'acquire', b'key_bar', b'client_bar', b'30'])
            self.assertEqual(sock.recv(), b'ok')
            sock.send_multipart([b'release', b'key_foo', b'client_foo'])
            self.assertEqual(sock.recv(), b'ok')
            sock.send_multipart([b'release', b'key_bar', b'client_bar'])
            self.assertEqual(sock.recv(), b'ok')

            # The same client acquiring different locks should be able to:
            sock.send_multipart([b'acquire', b'key_foo', b'client_foo', b'30'])
            self.assertEqual(sock.recv(), b'ok')
            sock.send_multipart([b'acquire', b'key_bar', b'client_foo', b'30'])
            self.assertEqual(sock.recv(), b'ok')
            sock.send_multipart([b'release', b'key_foo', b'client_foo'])
            self.assertEqual(sock.recv(), b'ok')
            sock.send_multipart([b'release', b'key_bar', b'client_foo'])
            self.assertEqual(sock.recv(), b'ok')


class ImplementationUnitTests(unittest.TestCase):
    def test_cant_call_task_twice(self):
        task = Task(1, lambda: None)
        task()
        with self.assertRaises(RuntimeError):
            task()

    def test_queue(self):
        # Test insert order:
        queue = TaskQueue()
        task1 = Task(1, lambda: None)
        task2 = Task(2, lambda: None)
        task3 = Task(3, lambda: None)

        queue.add(task1)
        queue.add(task3)
        queue.add(task2)

        self.assertIs(queue[0], task3)
        self.assertIs(queue[1], task2)
        self.assertIs(queue[2], task1)

        # Test correct task pops:
        self.assertIs(queue.pop(), task1)

        # test cancel:
        queue.cancel(task2)
        self.assertEqual(queue, [task3])

    def test_lock_errors(self):
        class FakeServer(object):
            active_locks = {}

        server = FakeServer()
        lock = Lock.instance(b'key_foo', server)

        # Can't acquire if already held:
        self.assertTrue(lock.acquire(b'client_foo', read_only=False))
        with self.assertRaises(ValueError):
            lock.acquire(b'client_foo', read_only=False)
        self.assertEqual(lock.release(b'client_foo'), set())

        # Can't re-use a lock instance after all clients released:
        with self.assertRaises(RuntimeError):
            lock.acquire(b'client_foo', read_only=False)
        with self.assertRaises(RuntimeError):
            lock.release(b'client_foo')
        with self.assertRaises(RuntimeError):
            lock.give_up(b'client_foo')

        lock = Lock.instance(b'key_foo', server)
        # Can't release if not acquired:
        with self.assertRaises(ValueError):
            lock.release(b'client_foo')

        lock = Lock.instance(b'key_foo', server)
        # Can't acquire if already waiting:
        self.assertTrue(lock.acquire(b'client_foo', read_only=False))
        self.assertFalse(lock.acquire(b'client_bar', read_only=False))
        with self.assertRaises(ValueError):
            lock.acquire(b'client_bar', read_only=False)
        with self.assertRaises(ValueError):
            lock.acquire(b'client_bar', read_only=True)

class OtherTests(unittest.TestCase):
    def test_bind_to_port(self):
        # Run the server on a random port on localhost:
        import random

        for _ in range(1):
            port = random.randint(40000, 60000)
            server = ZMQLockServer(bind_address='tcp://127.0.0.1', port=port)
            server.run_in_thread()
            break
        else:
            raise RuntimeError("Couldn't bind to a specified port")
        try:
            with TemporaryREQSocket(port) as sock:
                # Reader acquires the lock:
                sock.send_multipart(
                    [b'acquire', b'key_foo', b'client_foo', b'30', b'read_only']
                )
                self.assertEqual(sock.recv(), b'ok')
                # Reader releases the lock:
                sock.send_multipart([b'release', b'key_foo', b'client_foo'])
                self.assertEqual(sock.recv(), b'ok')
        finally:
            server.stop()


def test_speed():
    with TemporaryREQSocket(7339) as sock:
        start_time = time.time()
        sock.send(b'hello')
        assert sock.recv() == b'hello'
        for _ in range(1000):
            sock.send(b'hello')
            assert sock.recv() == b'hello'
        print('hello:', (time.time() - start_time) / 1000 * 1e6, 'us')

        start_time = time.time()
        for _ in range(1000):
            sock.send_multipart([b'acquire', b'foo', b'client_foo', b'30'])
            assert sock.recv() == b'ok'
            sock.send_multipart([b'release', b'foo', b'client_foo'])
            assert sock.recv() == b'ok'
        print('acq/release:', (time.time() - start_time) / 1000 * 1e6, 'us')

        start_time = time.time()
        for _ in range(1000):
            with open('/etc/hosts'):
                pass
        print('open/close:', (time.time() - start_time) / 1000 * 1e6, 'us')

        start_time = time.time()
        for _ in range(1000):
            pass
        print('nothing:', (time.time() - start_time) / 1000 * 1e6, 'us')


# test_speed()

if __name__ == '__main__':

    output = 'test-reports'
    if PY2:
        output = output.encode('utf8')
    testRunner = xmlrunner.XMLTestRunner(output=output, verbosity=3)
    unittest.main(verbosity=3, testRunner=testRunner, exit=False)
