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

from zprocess.zlock.server import (
    ZMQLockServer,
    Lock,
    NotHeld,
    AlreadyWaiting,
    InvalidReentry,
    ERR_NOT_HELD,
    ERR_INVALID_REENTRY,
    ERR_CONCURRENT,
    ERR_INVALID_COMMAND,
    ERR_WRONG_NUM_ARGS,
    ERR_TIMEOUT_INVALID,
    ERR_READ_ONLY_WRONG,
)
import zprocess.zlock.server


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


class Client(object):
    """Context manager for a mock client"""

    def __init__(self, testcase, port, key, client_id):
        context = zmq.Context.instance()
        self.sock = context.socket(zmq.REQ)
        self.port = port
        self.key = key
        self.client_id = client_id
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

    def acquire(self, timeout=30, read_only=False):
        timeout = str(timeout).encode('utf8')
        msg = [b'acquire', self.key, self.client_id, timeout]
        if read_only:
            msg += [b'read_only']
        return self.send_multipart(msg)

    def release(self):
        return self.send_multipart([b'release', self.key, self.client_id])

    def assertReceived(self, response):
        self.testcase.assertEqual(self.recv(), response)

    def assertNoResponse(self):
        self.testcase.assertEqual(self.poll(100, zmq.POLLIN), 0)

    def __exit__(self, *args, **kwargs):
        self.sock.close()


class ZLockServerTests(unittest.TestCase):
    def setUp(self):
        # Run the server on a random port on localhost:
        self.server = ZMQLockServer(bind_address='tcp://127.0.0.1', silent=True)
        self.server.run_in_thread()
        self.port = self.server.port

    def tearDown(self):
        self.server.stop()
        self.server = None
        self.port = None

    def client(self, key, client_id):
        return Client(self, self.port, key, client_id)

    def test_hello(self):
        with self.client(None, None) as client:
            client.send(b'hello')
            client.assertReceived(b'hello')

    def test_protocol(self):
        with self.client(None, None) as client:
            client.send(b'protocol')
            client.assertReceived(b'1.1.0')

    def test_uncontested_reader(self):
        with self.client(b'key_foo', b'client_foo') as reader:
            # Reader acquires the lock:
            reader.acquire(read_only=True)
            reader.assertReceived(b'ok')
            # Reader releases the lock:
            reader.release()
            self.assertEqual(reader.recv(), b'ok')

    def test_uncontested_writer(self):
        with self.client(b'key_foo', b'client_foo') as writer:
            # Writer acquires the lock:
            writer.acquire()
            self.assertEqual(writer.recv(), b'ok')
            # Writer releases the lock:
            writer.release()
            self.assertEqual(writer.recv(), b'ok')

    def test_multiple_readers(self):
        reader1 = self.client(b'key_foo', b'client_foo')
        reader2 = self.client(b'key_foo', b'client_bar')
        with reader1, reader2:
            # Reader acquires the lock:
            reader1.acquire(read_only=True)
            reader1.assertReceived(b'ok')
            # Another reader acquires the lock:
            reader2.acquire(read_only=True)
            reader2.assertReceived(b'ok')
            # Both release the lock:
            reader1.release()
            reader1.assertReceived(b'ok')
            reader2.release()
            reader2.assertReceived(b'ok')

    def test_reader_then_writer(self):
        reader = self.client(b'key_foo', b'client_foo')
        writer = self.client(b'key_foo', b'client_bar')
        with reader, writer:
            # Reader acquires the lock:
            reader.acquire(read_only=True)
            reader.assertReceived(b'ok')
            # Writer is blocked trying to acquire the lock:
            writer.acquire()
            writer.assertNoResponse()
            # Reader releases the lock:
            reader.release()
            reader.assertReceived(b'ok')
            # Writer should now get the lock:
            writer.assertReceived(b'ok')
            # Writer releases the lock:
            writer.release()
            self.assertEqual(writer.recv(), b'ok')

    def test_waiting_writer_blocks_new_readers(self):
        reader1 = self.client(b'key_foo', b'reader1')
        reader2 = self.client(b'key_foo', b'reader2')
        writer = self.client(b'key_foo', b'writer')
        with reader1, reader2, writer:
            # Reader acquires the lock:
            reader1.acquire(read_only=True)
            reader1.assertReceived(b'ok')
            # Writer is blocked trying to acquire the lock:
            writer.acquire()
            writer.assertNoResponse()
            # Another reader attempts to acquire the lock but is blocked:
            reader2.acquire(read_only=True)
            reader2.assertNoResponse()
            # The first reader releases the lock:
            reader1.release()
            reader1.assertReceived(b'ok')
            # The writer should get the lock now:
            writer.assertReceived(b'ok')
            # Not the other reader though:
            reader2.assertNoResponse()
            # The writer releases the lock:
            writer.release()
            writer.assertReceived(b'ok')
            # Now the second reader should get the lock:
            reader2.assertReceived(b'ok')
            # Which it then releases:
            reader2.release()
            reader2.assertReceived(b'ok')

    def test_concurrent_error(self):
        writer1 = self.client(b'key_foo', b'writer1')
        writer2 = self.client(b'key_foo', b'writer2')
        writer2_copy = self.client(b'key_foo', b'writer2')

        with writer1, writer2, writer2_copy:
            # Writer acquires the lock
            writer1.acquire()
            writer1.assertReceived(b'ok')
            # Another writer is blocked trying to acquire:
            writer2.acquire()
            writer2.assertNoResponse()
            # The second writer doesn't wait for a reply before retrying:
            writer2_copy.acquire()
            # It gets an error:
            writer2_copy.assertReceived(ERR_CONCURRENT)
            # The second client then calls release before waiting for a response:
            writer2_copy.release()
            # It gets an error:
            writer2_copy.assertReceived(ERR_CONCURRENT)
            # The first writer releases the lock:
            writer1.release()
            writer1.assertReceived(b'ok')
            # The second writer gets it now:
            writer2.assertReceived(b'ok')
            # And releases it:
            writer2.release()
            writer2.assertReceived(b'ok')

    def test_change_mind(self):
        reader = self.client(b'key_foo', b'reader')
        mind_changer = self.client(b'key_foo', b'mind_changer')
        with reader, mind_changer:
            # Reader acquires the lock
            reader.acquire(read_only=True)
            reader.assertReceived(b'ok')
            # Speed up the response:
            with monkeypatch(zprocess.zlock.server, 'MAX_RESPONSE_TIME', 0.1):
                # Another writer tries to acquire but is told to retry
                mind_changer.acquire()
                mind_changer.assertReceived(b'retry')
            # It retries, but now it wants a read-only lock:
            mind_changer.acquire(read_only=True)
            # It should get it, since the other client is a reader:
            mind_changer.assertReceived(b'ok')
            # Both clients release:
            mind_changer.release()
            mind_changer.assertReceived(b'ok')
            reader.release()
            reader.assertReceived(b'ok')

    def test_absentee_acquire_release(self):
        writer1 = self.client(b'key_foo', b'writer1')
        writer2 = self.client(b'key_foo', b'writer2')
        with writer1, writer2:
            # Client acquires the lock:
            writer1.acquire()
            writer1.assertReceived(b'ok')
            # Speed up the response:
            with monkeypatch(zprocess.zlock.server, 'MAX_RESPONSE_TIME', 0.1):
                # Another client tries to acquire but is told to retry
                writer2.acquire()
                writer2.assertReceived(b'retry')
            # First client releases the lock, now the second client has the lock in
            # absentia:
            writer1.release()
            writer1.assertReceived(b'ok')
            # Second client calls release, even though it hasn't retried yet:
            writer2.release()
            # It should get an error, since it didn't follow protocol in finishing
            # acquiring the lock:
            writer2.assertReceived(ERR_NOT_HELD)

    def test_absentee_acquire_timeout(self):
        client1 = self.client(b'key_foo', b'client1')
        client2 = self.client(b'key_foo', b'client2')
        with client1, client2:
            # First client acquires the lock:
            client1.acquire()
            client1.assertReceived(b'ok')
            # Speed up the response:
            with monkeypatch(zprocess.zlock.server, 'MAX_RESPONSE_TIME', 0.1):
                # Second client tries to acquire the lock, but is told to retry:
                client2.acquire()
            client2.assertReceived(b'retry')
            # Before the second client retries, the first client releases the lock.
            # The second client should now have the lock in absentia.
            with monkeypatch(zprocess.zlock.server, 'MAX_ABSENT_TIME', 0.2):
                client1.release()
                client1.assertReceived(b'ok')
            # Before the second client retries, the first client asks for the lock
            # again:
            client1.acquire()
            # It doesn't work at first, because the second client has the lock:
            client1.assertNoResponse()
            # But since the second client is not retrying, its lock times out
            # and the first client gets it:
            client1.assertReceived(b'ok')
            # Then it releases it:
            client1.release()
            client1.assertReceived(b'ok')

            # The second client lost its spot in the queue, but should be able to
            # try again to acquire and release the lock as normal:
            client2.acquire()
            client2.assertReceived(b'ok')
            client2.release()
            client2.assertReceived(b'ok')

    def test_timeout(self):
        with self.client(b'key_foo', b'client_foo') as client:
            # Client acquires the lock with a short timeout
            client.acquire(timeout=0.1)
            client.assertReceived(b'ok')
            # Client waits longer than the timeout before releasing:
            time.sleep(0.2)
            client.release()
            # And gets an error:
            client.assertReceived(ERR_NOT_HELD)

    def test_reentry(self):
        with self.client(b'key_foo', b'client_foo') as client:
            for first, second in [(True, True), (False, False), (False, True)]:
                # Client acquires the lock:
                client.acquire(read_only=first)
                client.assertReceived(b'ok')
                # Client reentrantly requests the lock again:
                client.acquire(read_only=second)
                client.assertReceived(b'ok')
                # Release:
                client.release()
                client.assertReceived(b'ok')
                client.release()
                client.assertReceived(b'ok')
                # Error if the client releases too far:
                client.release()
                client.assertReceived(ERR_NOT_HELD)

    def test_reentrant_contending_writers(self):
        reader = self.client(b'key_foo', b'reader')
        writer = self.client(b'key_foo', b'writer')
        with reader, writer:
            with monkeypatch(zprocess.zlock.server, 'MAX_RESPONSE_TIME', 0.1):
                # Reader acquires the lock:
                reader.acquire()
                reader.assertReceived(b'ok')
                # Writer tries to acquire the lock, but is told to retry:
                writer.acquire()
                writer.assertReceived(b'retry')
                # Reader reentrantly acquires the lock:
                reader.acquire()
                reader.assertReceived(b'ok')
                # Writer still told to retry:
                writer.acquire()
                writer.assertReceived(b'retry')
                # Reader releases fully:
                reader.release()
                reader.assertReceived(b'ok')
                reader.release()
                reader.assertReceived(b'ok')
                # Now the writer gets it:
                writer.acquire()
                writer.assertReceived(b'ok')
                # And releases:
                writer.release()
                writer.assertReceived(b'ok')
                # Both shouldn't be able to release more:
                writer.release()
                writer.assertReceived(ERR_NOT_HELD)
                reader.release()
                reader.assertReceived(ERR_NOT_HELD)

    def test_reentrant_reader_with_waiting_writer(self):
        reader = self.client(b'key_foo', b'reader')
        writer = self.client(b'key_foo', b'writer')
        with reader, writer:
            with monkeypatch(zprocess.zlock.server, 'MAX_RESPONSE_TIME', 0.1):
                # Reader acquires the lock:
                reader.acquire(read_only=True)
                reader.assertReceived(b'ok')
                # Writer tries to acquire the lock, but is told to retry:
                writer.acquire()
                writer.assertReceived(b'retry')
                # Reader reentrantly acquires the lock:
                reader.acquire(read_only=True)
                reader.assertReceived(b'ok')
                # Writer still told to retry:
                writer.acquire()
                writer.assertReceived(b'retry')
                # Reader releases fully:
                reader.release()
                reader.assertReceived(b'ok')
                reader.release()
                reader.assertReceived(b'ok')
                # Now the writer gets it:
                writer.acquire()
                writer.assertReceived(b'ok')
                # And releases:
                writer.release()
                writer.assertReceived(b'ok')
                # Both shouldn't be able to release more:
                writer.release()
                writer.assertReceived(ERR_NOT_HELD)
                reader.release()
                reader.assertReceived(ERR_NOT_HELD)

    def test_invalid_reentry(self):
        with self.client(b'key_foo', b'client_foo') as client:
            # Client acquires the lock as reader:
            client.acquire(read_only=True)
            client.assertReceived(b'ok')
            # Client re-entrantly requests the lock as a writer:
            client.acquire()
            client.assertReceived(ERR_INVALID_REENTRY)

    def test_malformed_requests(self):
        with self.client(b'key_foo', b'client_foo') as client:
            # Invalid command:
            client.send_multipart([b'frobulate', b'key_foo', b'client_foo', b'30'])
            client.assertReceived(ERR_INVALID_COMMAND)
            # read_only wrong:
            client.send_multipart(
                [b'acquire', b'key_foo', b'client_foo', b'30', b'not_going_to_write']
            )
            client.assertReceived(ERR_READ_ONLY_WRONG)
            # Too few args to acquire:
            client.send_multipart([b'acquire'])
            client.assertReceived(ERR_WRONG_NUM_ARGS)
            # Too many args to acquire:
            client.send_multipart([b'acquire', b'1', b'2', b'3', b'4', b'5'])
            client.assertReceived(ERR_WRONG_NUM_ARGS)
            # Timeout not a number:
            client.send_multipart([b'acquire', b'key_foo', b'client_foo', b'fs'])
            client.assertReceived(ERR_TIMEOUT_INVALID)
            # Timeout not a valid number:
            client.send_multipart([b'acquire', b'key_foo', b'client_foo', b'-inf'])
            client.assertReceived(ERR_TIMEOUT_INVALID)
            # Too few args to release:
            client.send_multipart([b'release'])
            client.assertReceived(ERR_WRONG_NUM_ARGS)
            # Too many args to release:
            client.send_multipart([b'release', b'1', b'2', b'3', b'4', b'5'])
            client.assertReceived(ERR_WRONG_NUM_ARGS)

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
        client1 = self.client(b'key_foo', b'client1')
        client2 = self.client(b'key_foo', b'client2')
        with client1, client2:
            # First client acquires the lock:
            client1.acquire()
            client1.assertReceived(b'ok')
            # Speed up the response:
            with monkeypatch(zprocess.zlock.server, 'MAX_RESPONSE_TIME', 0.1):
                # Second client tries to acquire the lock, but is told to retry:
                client2.acquire()
            client2.assertReceived(b'retry')
            # Before the second client retries, the first client releases the lock:
            client1.release()
            client1.assertReceived(b'ok')
            # The second client should receive the lock upon retrying:
            client2.acquire()
            client2.assertReceived(b'ok')

    def test_retry(self):
        writer1 = self.client(b'key_foo', b'writer1')
        writer2 = self.client(b'key_foo', b'writer2')
        with writer1, writer2:
            # First client acquires the lock:
            writer1.acquire()
            writer1.assertReceived(b'ok')
            # Speed up the response:
            with monkeypatch(zprocess.zlock.server, 'MAX_RESPONSE_TIME', 0.1):
                # Second client tries to acquire the lock but is told to retry:
                writer2.acquire()
                writer2.assertReceived(b'retry')
            # Second client retries:
            writer2.acquire()
            # First client releases:
            writer1.release()
            writer1.assertReceived(b'ok')
            # Second client gets it:
            writer2.assertReceived(b'ok')

    def test_absent_give_up(self):
        client1 = self.client(b'key_foo', b'client1')
        client2 = self.client(b'key_foo', b'client2')
        with client1, client2:
            for read_only in [True, False]:
                # First client acquires the lock:
                client1.acquire()
                client1.assertReceived(b'ok')
                # Speed up the response and away timeout:
                with monkeypatch(zprocess.zlock.server, 'MAX_RESPONSE_TIME', 0.1):
                    with monkeypatch(zprocess.zlock.server, 'MAX_ABSENT_TIME', 0.1):
                        # Second client tries to acquire the lock but is told to retry:
                        client2.acquire(read_only=read_only)
                        client2.assertReceived(b'retry')
                        # Second client waits too long and loses its spot in the queue
                        time.sleep(0.2)
                        # First client releases the lock:
                        client1.release()
                        client1.assertReceived(b'ok')
                        # And then acquires it again, sucessfully since client 1 has
                        # given up:
                        client1.acquire()
                        client1.assertReceived(b'ok')
                        # Second client retries:
                        client2.acquire(read_only=read_only)
                        # But is told to wait since it lost its spot in the queue:
                        client2.assertReceived(b'retry')
                        # It immediately retries:
                        client2.acquire(read_only=read_only)
                        # First client releases:
                        client1.release()
                        client1.assertReceived(b'ok')
                        # Then second client gets the lock:
                        client2.assertReceived(b'ok')
                        # And finally releases:
                        client2.release()
                        client2.assertReceived(b'ok')

    def test_locks_independent(self):
        client1 = self.client(b'key_foo', b'client1')
        client2 = self.client(b'key_bar', b'client2')
        client1_different_lock = self.client(b'key_bar', b'client1')
        with client1, client2, client1_different_lock:
            # Two clients acquiring different locks should be able to:
            client1.acquire()
            client1.assertReceived(b'ok')
            client2.acquire()
            client2.assertReceived(b'ok')
            client1.release()
            client1.assertReceived(b'ok')
            client2.release()
            client2.assertReceived(b'ok')

            # The same client acquiring different locks should be able to:
            client1.acquire()
            client1.assertReceived(b'ok')
            client1_different_lock.acquire()
            client1_different_lock.assertReceived(b'ok')
            client1.release()
            client1.assertReceived(b'ok')
            client1_different_lock.release()
            client1_different_lock.assertReceived(b'ok')


class ImplementationUnitTests(unittest.TestCase):

    def test_lock_errors(self):
        class FakeServer(object):
            active_locks = {}

        server = FakeServer()
        lock = Lock.instance(b'key_foo', server)

        # Can't acquire as read-write if already held read-only:
        self.assertTrue(lock.acquire(b'client_foo', read_only=True))
        with self.assertRaises(InvalidReentry):
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
        with self.assertRaises(NotHeld):
            lock.release(b'client_foo')

        lock = Lock.instance(b'key_foo', server)
        # Can't acquire if already waiting:
        self.assertTrue(lock.acquire(b'client_foo', read_only=False))
        self.assertFalse(lock.acquire(b'client_bar', read_only=False))
        with self.assertRaises(AlreadyWaiting):
            lock.acquire(b'client_bar', read_only=False)
        with self.assertRaises(AlreadyWaiting):
            lock.acquire(b'client_bar', read_only=True)


class OtherTests(unittest.TestCase):
    def test_bind_to_port(self):
        # Run the server on a random port on localhost:
        import random

        for _ in range(1):
            port = random.randint(40000, 60000)
            server = ZMQLockServer(
                bind_address='tcp://127.0.0.1', port=port, silent=True
            )
            server.run_in_thread()
            break
        else:
            raise RuntimeError("Couldn't bind to a specified port")
        try:
            with Client(self, port, b'key_foo', b'client_foo') as reader:
                # Reader acquires the lock:
                reader.acquire(read_only=True)
                reader.assertReceived(b'ok')
                # Reader releases the lock:
                reader.release()
                reader.assertReceived(b'ok')
        finally:
            server.stop()

    def test_stop_unstarted(self):
        # Can't stop a server that's not running:
        server = ZMQLockServer(bind_address='tcp://127.0.0.1', silent=True)
        with self.assertRaises(RuntimeError):
            server.stop()


def test_speed():
    with Client(None, 7339, b'key_foo', b'client_foo') as client:
        start_time = time.time()
        client.send(b'hello')
        assert client.recv() == b'hello'
        for _ in range(1000):
            client.send(b'hello')
            assert client.recv() == b'hello'
        print('hello:', (time.time() - start_time) / 1000 * 1e6, 'us')

        start_time = time.time()
        for _ in range(1000):
            client.send_multipart([b'acquire', b'foo', b'client_foo', b'30'])
            assert client.recv() == b'ok'
            client.send_multipart([b'release', b'foo', b'client_foo'])
            assert client.recv() == b'ok'
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
    unittest.main(verbosity=3, testRunner=testRunner, exit=not sys.flags.interactive)
