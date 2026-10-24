import unittest
from pathlib import Path
import sys
import os
import time
import signal
import threading
import subprocess
import tempfile
import json
from unittest.mock import patch

import zmq
import pytest

THIS_DIR = Path(__file__).absolute().parent

# Add project root to import path
PROJECT_ROOT = THIS_DIR.parent
if PROJECT_ROOT not in [Path(s).absolute() for s in sys.path]:
    sys.path.insert(0, str(PROJECT_ROOT))

import zprocess
zprocess._silent = True
from zprocess import (ZMQServer, Process, TimeoutError, RichStreamHandler, rich_print,
                      raise_exception_in_thread, zmq_get, zmq_push, zmq_get_raw,
                      ExternalBroker, KillLock, interrupt_main_thread)
import zprocess.clientserver as clientserver
from zprocess.clientserver import _typecheck_or_convert_data
from zprocess.process_tree import EventBroker, ReadQueue, ProcessTree
default_process_tree = ProcessTree(allow_insecure=True)
zprocess.set_default_process_tree(default_process_tree)
shared_secret = default_process_tree.shared_secret
from zprocess.security import SecureContext
from zprocess.tasks import Task, TaskQueue


class TestError(Exception):
    pass


class RaiseExceptionInThreadTest(unittest.TestCase):

    def setUp(self):
        # Mock threading.Thread to just run a function in the main thread:
        class MockThread(object):
            used = False
            def __init__(self, target, args):
                self.target = target
                self.args = args
            def start(self):
                MockThread.used = True
                self.target(*self.args)
        self.mock_thread = MockThread
        self.orig_thread = threading.Thread
        threading.Thread = MockThread

    def test_can_raise_exception_in_thread(self):
        try:
            raise TestError('test')
        except Exception:
            exc_info = sys.exc_info()
            with self.assertRaises(TestError):
                raise_exception_in_thread(exc_info)
            self.assertTrue(self.mock_thread.used)

    def tearDown(self):
        # Restore threading.Thread to what it should be
        threading.Thread = self.orig_thread


class  TypeCheckConvertTests(unittest.TestCase):
    """test the _typecheck_or_convert_data function"""

    def test_turns_None_into_empty_bytestring_raw(self):
        result = _typecheck_or_convert_data(None, 'raw')
        self.assertEqual(result, b'')

    def test_turns_None_into_empty_bytestring_multipart(self):
        result = _typecheck_or_convert_data(None, 'multipart')
        self.assertEqual(result, [b''])

    def test_wraps_bytestring_into_list_multipart(self):
        data = b'spam'
        result = _typecheck_or_convert_data(data, 'multipart')
        self.assertEqual(result, [data])

    def test_accepts_bytes_raw(self):
        data = b'spam'
        result = _typecheck_or_convert_data(data, 'raw')
        self.assertEqual(result, data)

    def test_accepts_list_of_bytes_multipart(self):
        data = [b'spam', b'ham']
        result = _typecheck_or_convert_data(data, 'multipart')
        self.assertEqual(result, data)

    def test_accepts_string_string(self):
        data = 'spam'
        result = _typecheck_or_convert_data(data, 'string')
        self.assertEqual(result, data)

    def test_accepts_pyobj_pyobj(self):
        data = {'spam': ['ham'], 'eggs': True}
        result = _typecheck_or_convert_data(data, 'pyobj')
        self.assertEqual(result, data)

    def test_rejects_string_raw(self):
        data = 'spam'
        with self.assertRaises(TypeError):
            _typecheck_or_convert_data(data, 'raw')

    def test_rejects_string_multipart(self):
        data = [b'spam', 'ham']
        with self.assertRaises(TypeError):
            _typecheck_or_convert_data(data, 'multipart')

    def test_rejects_pyobj_string(self):
        data = {'spam': ['ham'], 'eggs': True}
        with self.assertRaises(TypeError):
            _typecheck_or_convert_data(data, 'string')

    def test_rejects_invalid_send_type(self):
        data = {'spam': ['ham'], 'eggs': True}
        with self.assertRaises(ValueError):
            _typecheck_or_convert_data(data, 'invalid_send_type')


class TestProcess(Process):
    def run(self):
        item = self.from_parent.get()
        x, y = item
        sys.stdout.write(repr(x))
        sys.stderr.write(y)
        self.to_parent.put(item)
        os.system('echo hello from echo')
        # Wait some time here to reduce the chance of output arriving out of order,
        # which would break the tests even though we don't actually guarantee that the
        # order will come out right.
        time.sleep(0.2)

        # And now test logging:
        import logging
        logger = logging.Logger('test')
        logger.setLevel(logging.DEBUG)
        logger.addHandler(RichStreamHandler())
        logger.info('this is a log message')

        # And now test rich printing
        rich_print('some test text', color='#123456', bold=True, italic=False)



class ProcessClassTests(unittest.TestCase):

    def setUp(self):
        """Create a subprocess with output redirection to a zmq port"""
        context = SecureContext.instance(shared_secret=shared_secret)
        self.redirection_sock = context.socket(zmq.PULL)
        redirection_port = self.redirection_sock.bind_to_random_port(
                               'tcp://127.0.0.1')
        self.process = TestProcess(output_redirection_port=redirection_port)

    def test_process(self):
        to_child, from_child = self.process.start()
        # Check the child process is running:
        self.assertIs(self.process.child.poll(), None)

        # Send some data:
        x = [('spam', ['ham']), ('eggs', True)]
        y = 'über'
        data = (x, y)
        to_child.put(data)

        # Subprocess should send back the data unmodified:
        recv_data = from_child.get(timeout=1)
        self.assertEqual(recv_data, data)

        # Subprocess should not send any more data, expect TimeoutError:
        with self.assertRaises(TimeoutError):
            from_child.get(timeout=0.1)

        # Check we recieved its stdout and stderr:
        self.assertEqual(self.redirection_sock.poll(1000), zmq.POLLIN)
        self.assertEqual(self.redirection_sock.recv_multipart(),
                         [b'stdout', repr(x).encode('utf8')])
        self.assertEqual(self.redirection_sock.poll(1000), zmq.POLLIN)
        self.assertEqual(self.redirection_sock.recv_multipart(),
                         [b'stderr', y.encode('utf8')])
        # And the shell output:
        self.assertEqual(self.redirection_sock.poll(1000), zmq.POLLIN)

        self.assertEqual(
            [p.strip() for p in self.redirection_sock.recv_multipart()],
            [b'stdout', b'hello from echo']
        )

        # And the formatted logging:
        self.assertEqual(self.redirection_sock.recv_multipart(),
                         [b'INFO', b'this is a log message\n'])

        # And the formatted printing:
        import ast
        charfmt_repr, text = self.redirection_sock.recv_multipart()
        self.assertEqual(ast.literal_eval(charfmt_repr.decode('utf8')), ('#123456', True, False))
        self.assertEqual(text, b'some test text\n')

        # And no more...
        self.assertEqual(self.redirection_sock.poll(100), 0)

    def tearDown(self):
        self.process.terminate()
        self.redirection_sock.close()


class InterruptMainThreadTests(unittest.TestCase):
    """interrupt_main_thread() raises KeyboardInterrupt in the main thread. A blocking
    call that Python can wake returns at once; one that it cannot wake returns in its
    own time and the exception is raised then."""

    # Delay from the start of the blocking call to the interrupt:
    DELAY = 0.2
    # An interrupt arriving later than this did not wake the blocking call:
    PROMPT = 2

    def setUp(self):
        self.prev_handler = signal.signal(signal.SIGINT, signal.default_int_handler)

    def tearDown(self):
        signal.signal(signal.SIGINT, self.prev_handler)

    def _time_to_interrupt(self, blocking_call):
        """Run blocking_call in this (the main) thread, interrupt it after DELAY
        seconds from another thread and return the time at which KeyboardInterrupt was
        raised, measured from the start of the call."""
        timer = threading.Timer(self.DELAY, interrupt_main_thread)
        start = time.monotonic()
        timer.start()
        try:
            with self.assertRaises(KeyboardInterrupt):
                blocking_call()
                # A pending interrupt that did not wake the call is raised here:
                time.sleep(0.01)
        finally:
            timer.cancel()
        return time.monotonic() - start

    def test_sleep(self):
        elapsed = self._time_to_interrupt(lambda: time.sleep(10))
        self.assertLess(elapsed, self.PROMPT)

    def test_lock_acquire(self):
        lock = threading.Lock()
        lock.acquire()
        elapsed = self._time_to_interrupt(lambda: lock.acquire(timeout=4))
        if os.name == 'nt' and sys.version_info < (3, 14):
            # The wait is not woken; the exception is raised after the timeout.
            self.assertGreater(elapsed, self.PROMPT)
        else:
            self.assertLess(elapsed, self.PROMPT)

    def test_zmq_poll(self):
        sock = zmq.Context.instance().socket(zmq.PULL)
        sock.bind('tcp://127.0.0.1:0')
        try:
            elapsed = self._time_to_interrupt(lambda: sock.poll(4000))
        finally:
            sock.close(linger=0)
        if os.name == 'nt':
            # The poll is not woken; the exception is raised after it returns.
            self.assertGreater(elapsed, self.PROMPT)
        else:
            self.assertLess(elapsed, self.PROMPT)

    def test_python_loop(self):
        def loop():
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                pass

        elapsed = self._time_to_interrupt(loop)
        self.assertLess(elapsed, self.PROMPT)

    def test_requires_a_python_sigint_handler(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        with self.assertRaises(RuntimeError):
            interrupt_main_thread()


class KillLockTests(unittest.TestCase):
    def test_is_held(self):
        lock = KillLock()
        self.assertFalse(lock.is_held())
        with lock:
            self.assertTrue(lock.is_held())
            with lock:
                self.assertTrue(lock.is_held())
            self.assertTrue(lock.is_held())
        self.assertFalse(lock.is_held())

    def test_exit_when_released_waits_for_release(self):
        lock = KillLock()
        exited = threading.Event()
        with patch('os._exit', lambda status: exited.set()):
            with lock:
                thread = threading.Thread(target=lock.exit_when_released)
                thread.daemon = True
                thread.start()
                self.assertFalse(exited.wait(0.5))
            self.assertTrue(exited.wait(2))
            thread.join(2)


class ProcessTerminateTests(unittest.TestCase):
    def test_terminate_ignores_local_wait_timeout(self):
        class DummyChild(object):
            def terminate(self, **kwargs):
                pass

            def wait(self, timeout=None, **kwargs):
                raise subprocess.TimeoutExpired(cmd='dummy', timeout=timeout)

        process = Process.__new__(Process)
        process.child = DummyChild()
        process.interrupt_startup = lambda reason=None: None

        # Should not leak subprocess.TimeoutExpired for a local child wait timeout:
        process.terminate(wait_timeout=0.1)


class HeartbeatClientTestProcess(Process):
    """For testing that subprocesses are behaving correcly re. heartbeats"""
    def run(self):
        self.from_parent.get()
        # If the parent sends a message, acquire the kill lock for 3 seconds:
        with self.kill_lock:
            time.sleep(3)
        time.sleep(10)


class HeartbeatServerTestProcess(Process):
    """For testing that parent processes are behaving correcly re. heartbeats"""
    def run(self):
        # We'll send heartbeats of our own, independent of the HeartbeatClient
        # already running in this process:
        shared_secret = self.process_tree.shared_secret
        context = SecureContext.instance(shared_secret=shared_secret)
        sock = context.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        server_port = self.from_parent.get()
        sock.connect('tcp://127.0.0.1:%s' % server_port)
        # Send a heartbeat with whatever data:
        data = b'heartbeat_data'
        sock.send(data)
        if sock.poll(1000):
            response = sock.recv()
            # Tell the parent whether things were as expected:
            if response == data:
                self.to_parent.put(True)
                time.sleep(1) # Ensure it sends before we return
                return
        self.to_parent.put(False)
        time.sleep(1) # Ensure it sends before we return


class ScriptedHeartbeatServer(object):
    def __init__(self, shared_secret, script, default='good'):
        import zmq

        self.context = SecureContext.instance(shared_secret=shared_secret)
        self.script = list(script)
        self.default = default
        self.actions = []
        self.request_times = []
        self._lock = threading.Lock()
        self._running = True
        self._request_event = threading.Event()
        self.sock = self.context.socket(zmq.ROUTER)
        self.port = self.sock.bind_to_random_port('tcp://127.0.0.1')
        self.thread = threading.Thread(target=self.mainloop)
        self.thread.daemon = True
        self.thread.start()

    def _next_action(self):
        with self._lock:
            if self.script:
                action = self.script.pop(0)
            else:
                action = self.default
            self.actions.append(action)
            return action

    def request_count(self):
        with self._lock:
            return len(self.request_times)

    def wait_for_requests(self, count, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.request_count() >= count:
                return True
            self._request_event.wait(min(0.1, deadline - time.time()))
            self._request_event.clear()
        return self.request_count() >= count

    def inter_request_intervals(self):
        with self._lock:
            times = list(self.request_times)
        return [later - earlier for earlier, later in zip(times, times[1:])]

    def mainloop(self):
        while self._running:
            try:
                if not self.sock.poll(100):
                    continue
                msg = self.sock.recv_multipart()
                with self._lock:
                    self.request_times.append(time.time())
                self._request_event.set()
                action = self._next_action()
                payload = msg[-1]
                if action == 'good':
                    self.sock.send_multipart(msg[:-1] + [payload])
                elif action == 'malformed':
                    self.sock.send_multipart(msg[:-1] + [payload + b'wrong'])
                elif action == 'drop':
                    continue
                else:
                    raise ValueError(action)
            except zmq.ZMQError:
                if self._running:
                    raise

    def close(self):
        # Stop the thread before closing the socket: a zmq socket is not thread-safe,
        # and closing it while the thread polls it can crash the process.
        self._running = False
        self.thread.join(timeout=1)
        self.sock.close(linger=0)


class HeartbeatTests(unittest.TestCase):
    def setUp(self):
        self.heartbeat_server = None

    def _process_tree(
        self, allowed_missed_heartbeats=None, heartbeat_interval=None
    ):
        if allowed_missed_heartbeats is None and heartbeat_interval is None:
            return default_process_tree

        class ConfiguredHeartbeatProcessTree(type(default_process_tree)):
            def subprocess(tree_self, *args, **kwargs):
                if allowed_missed_heartbeats is not None:
                    kwargs.setdefault(
                        'allowed_missed_heartbeats', allowed_missed_heartbeats
                    )
                if heartbeat_interval is not None:
                    kwargs.setdefault('heartbeat_interval', heartbeat_interval)
                return super(
                    ConfiguredHeartbeatProcessTree, tree_self
                ).subprocess(*args, **kwargs)

        process_tree = ConfiguredHeartbeatProcessTree(
            shared_secret=shared_secret,
            allow_insecure=default_process_tree.allow_insecure,
            zlock_host=default_process_tree.zlock_host,
            zlock_port=default_process_tree.zlock_port,
            zlog_host=default_process_tree.zlog_host,
            zlog_port=default_process_tree.zlog_port,
        )
        process_tree.heartbeat_server = self.heartbeat_server
        return process_tree

    def _start_heartbeat_process(
        self,
        allowed_missed_heartbeats=None,
        heartbeat_interval=None,
        script=None,
        default='good',
    ):
        self.heartbeat_server = ScriptedHeartbeatServer(
            shared_secret, script or [], default=default
        )
        process_tree = self._process_tree(
            allowed_missed_heartbeats,
            heartbeat_interval=heartbeat_interval,
        )
        self.process = HeartbeatClientTestProcess(process_tree=process_tree)
        return self.process.start()

    def _assert_process_exits_within(self, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.process.child.poll() is not None:
                return
            time.sleep(0.1)
        self.fail('process did not exit within %.1f seconds' % timeout)

    def _assert_alive_after_requests(self, request_count, timeout):
        self.assertTrue(
            self.heartbeat_server.wait_for_requests(request_count, timeout),
            'server did not receive %d heartbeats' % request_count,
        )
        time.sleep(0.2)
        self.assertIs(self.process.child.poll(), None)

    def _assert_intervals_are_timeouts(self, minimum_interval):
        intervals = self.heartbeat_server.inter_request_intervals()
        self.assertTrue(intervals, 'expected at least two heartbeat requests')
        self.assertTrue(
            all(interval >= minimum_interval for interval in intervals),
            'expected timeout-sized gaps, got %r' % intervals,
        )

    def _assert_intervals_are_immediate_retries(self, maximum_interval):
        intervals = self.heartbeat_server.inter_request_intervals()
        self.assertTrue(intervals, 'expected at least two heartbeat requests')
        self.assertTrue(
            all(interval <= maximum_interval for interval in intervals),
            'expected malformed replies to avoid timeout gaps, got %r' % intervals,
        )

    def test_subproc_lives_with_working_heartbeats(self):
        self._start_heartbeat_process(allowed_missed_heartbeats=5)
        self._assert_alive_after_requests(3, 5)

    def test_subproc_survives_missing_heartbeats_below_budget(self):
        for failures in range(1, 5):
            with self.subTest(failures=failures):
                self._start_heartbeat_process(
                    allowed_missed_heartbeats=failures + 1,
                    script=['drop'] * failures + ['good'],
                )
                self._assert_alive_after_requests(failures + 1, (failures + 1) * 3)
                self._assert_intervals_are_timeouts(1.5)
                self.process.terminate()
                self.heartbeat_server.close()
                self.process = None
                self.heartbeat_server = None

    def test_subproc_dies_after_1_to_5_missing_heartbeats(self):
        for failures in range(1, 6):
            with self.subTest(failures=failures):
                self._start_heartbeat_process(
                    allowed_missed_heartbeats=failures,
                    script=['drop'] * failures,
                )
                self.assertTrue(
                    self.heartbeat_server.wait_for_requests(failures, failures * 3),
                    'server did not receive %d dropped heartbeats' % failures,
                )
                self._assert_process_exits_within(3)
                if failures > 1:
                    self._assert_intervals_are_timeouts(1.5)
                self.process = None
                self.heartbeat_server.close()
                self.heartbeat_server = None

    def test_subproc_survives_malformed_heartbeats_below_budget(self):
        for failures in range(1, 5):
            with self.subTest(failures=failures):
                self._start_heartbeat_process(
                    allowed_missed_heartbeats=failures + 1,
                    script=['malformed'] * failures + ['good'],
                )
                self._assert_alive_after_requests(failures + 1, (failures + 1) * 2)
                self._assert_intervals_are_immediate_retries(1.5)
                self.process.terminate()
                self.heartbeat_server.close()
                self.process = None
                self.heartbeat_server = None

    def test_subproc_dies_after_1_to_5_malformed_heartbeats(self):
        for failures in range(1, 6):
            with self.subTest(failures=failures):
                self._start_heartbeat_process(
                    allowed_missed_heartbeats=failures,
                    script=['malformed'] * failures,
                )
                self.assertTrue(
                    self.heartbeat_server.wait_for_requests(failures, failures * 2),
                    'server did not receive %d malformed heartbeats' % failures,
                )
                self._assert_process_exits_within(2)
                if failures > 1:
                    self._assert_intervals_are_immediate_retries(1.5)
                self.process = None
                self.heartbeat_server.close()
                self.heartbeat_server = None

    def test_successful_heartbeat_resets_miss_counter(self):
        self._start_heartbeat_process(
            allowed_missed_heartbeats=2,
            script=['malformed', 'good', 'malformed', 'good'],
        )
        self._assert_alive_after_requests(4, 8)
        self._assert_intervals_are_immediate_retries(1.5)

    def test_configured_heartbeat_interval_controls_request_cadence(self):
        self._start_heartbeat_process(
            allowed_missed_heartbeats=3,
            heartbeat_interval=0.2,
        )
        self.assertTrue(self.heartbeat_server.wait_for_requests(3, 3))
        intervals = self.heartbeat_server.inter_request_intervals()[:2]
        self.assertTrue(
            all(0.1 <= interval < 0.8 for interval in intervals),
            'expected configured heartbeat-sized gaps, got %r' % intervals,
        )

    def test_subproc_survives_until_kill_lock_released(self):
        to_child, from_child = self._start_heartbeat_process(
            allowed_missed_heartbeats=1, script=['good', 'drop']
        )
        self.assertTrue(self.heartbeat_server.wait_for_requests(1, 3))
        # Tell child to acquire kill lock for 3 sec:
        to_child.put(None)
        # Don't respond to the heartbeat, process should still be alive 2 sec later
        time.sleep(2)
        # Process should be alive:
        self.assertIs(self.process.child.poll(), None)
        # After kill lock released, child should be terminated:
        self._assert_process_exits_within(3)
        # Process should be dead:
        self.assertIsNot(self.process.child.poll(), None)

    def test_parent_correctly_responds_to_heartbeats(self):
        # No mock server this time, we're testing the real one:
        default_process_tree.heartbeat_server = None
        self.process = HeartbeatServerTestProcess()
        to_child, from_child = self.process.start()
        to_child.put(default_process_tree.heartbeat_server.port)
        self.assertTrue(from_child.sock.poll(1000))
        self.assertTrue(from_child.get())

    def tearDown(self):
        if self.heartbeat_server is not None:
            self.heartbeat_server.close()
        try:
            if getattr(self, 'process', None) is not None:
                self.process.terminate()
        except Exception:
            pass # already dead

class TestEventProcess(Process):
    def run(self):
        event = self.process_tree.event('hello', role='post')
        event.post('1', data=u'boo')
        time.sleep(0.5)

class TestExternalEventProcess(Process):
    def run(self, broker_details):
        event = self.process_tree.event('hello', role='post', external_broker=broker_details)
        event.post('1', data=u'boo')
        time.sleep(0.5)



class TestConcurrentEventProcess(Process):
    def run(self, identifier):
        event = self.process_tree.event('concurrent', role='post')
        self.from_parent.get()
        event.post(identifier)
        time.sleep(5)


class EventTests(unittest.TestCase):
    def test_events(self):
        proc = TestEventProcess()
        event = default_process_tree.event('hello', role='wait')
        proc.start()
        try:
            data = event.wait('1', timeout=1)
            self.assertEqual(data, u'boo')
        finally:
            proc.terminate()

    def test_external_broker(self):
        broker = EventBroker(bind_address='tcp://127.0.0.1')
        broker_details = ExternalBroker('localhost', broker.in_port, broker.out_port)
        proc = TestExternalEventProcess()
        event = default_process_tree.event('hello', role='wait', external_broker=broker_details)
        proc.start(broker_details)
        try:
            data = event.wait('1', timeout=1)
            self.assertEqual(data, u'boo')
        finally:
            proc.terminate()


    def test_events_from_processes_started_concurrently(self):
        process_tree = ProcessTree(allow_insecure=True)
        n_processes = 8
        processes = [
            TestConcurrentEventProcess(process_tree) for _ in range(n_processes)
        ]
        queues = [None] * n_processes
        barrier = threading.Barrier(n_processes)

        def start(i):
            barrier.wait()
            queues[i] = processes[i].start(i)

        threads = [
            threading.Thread(target=start, args=(i,)) for i in range(n_processes)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        try:
            event = process_tree.event('concurrent', role='wait')
            for i, (to_child, _) in enumerate(queues):
                to_child.put(None)
                self.assertEqual(event.wait(i, timeout=5), None)
        finally:
            for process in processes:
                process.terminate()


class DefaultProcessTreeTests(unittest.TestCase):
    def test_no_default_process_tree(self):
        # A fresh interpreter, in which nothing has set a default:
        result = subprocess.run(
            [sys.executable, '-c', 'import zprocess; zprocess.Event("hello")'],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('No default ProcessTree', result.stderr)


class TaskTests(unittest.TestCase):
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


class ReadQueueTests(unittest.TestCase):
    def test_put_while_another_thread_gets(self):
        context = zmq.Context.instance()
        sock = context.socket(zmq.PAIR)
        sock.bind('inproc://zprocess-test-readqueue')
        queue = ReadQueue(sock)
        received = []
        getter = threading.Thread(target=lambda: received.append(queue.get(timeout=5)))
        getter.daemon = True
        getter.start()
        # Let the getter block in get():
        time.sleep(0.2)
        putter = threading.Thread(target=queue.put, args=('hello',))
        putter.daemon = True
        putter.start()
        putter.join(2)
        getter.join(2)
        try:
            self.assertFalse(putter.is_alive())
            self.assertEqual(received, ['hello'])
        finally:
            sock.close(linger=0)


class ClientServerTests(unittest.TestCase):

    def test_invalid_dtype_server_raises_valueerror(self):
        class InvalidServer(clientserver._ZMQServer):
            def setup_auth(self, context):
                return None

        class FakeSocket(object):
            def setsockopt(self, *args, **kwargs):
                pass

            def bind(self, *args, **kwargs):
                pass

            def close(self, *args, **kwargs):
                pass

        class FakeContext(object):
            def socket(self, *args, **kwargs):
                return FakeSocket()

        class FakePoller(object):
            def register(self, *args, **kwargs):
                pass

        with patch.object(clientserver.zmq, 'Context', FakeContext):
            with patch.object(clientserver.zmq, 'Poller', FakePoller):
                with self.assertRaisesRegex(ValueError, "invalid dtype invalid"):
                    InvalidServer(
                        port=1, dtype='invalid', bind_address='tcp://127.0.0.1'
                    )

    def test_invalid_dtype_sender_raises_valueerror(self):
        class FakeSocket(object):
            def setsockopt(self, *args, **kwargs):
                pass

            def connect(self, *args, **kwargs):
                pass

            def close(self, *args, **kwargs):
                pass

        class FakeContext(object):
            def socket(self, *args, **kwargs):
                return FakeSocket()

        class FakePoller(object):
            def register(self, *args, **kwargs):
                pass

        sender = clientserver._Sender(
            dtype='invalid', interruptor=clientserver.Interruptor()
        )

        with patch.object(clientserver.SecureContext, 'instance', return_value=FakeContext()):
            with patch.object(clientserver.zmq, 'Poller', FakePoller):
                with self.assertRaisesRegex(ValueError, "invalid dtype invalid"):
                    sender.new_socket('localhost', 1)

    def test_rep_server(self):
        class MyServer(ZMQServer):
            def handler(self, data):
                if data == 'error':
                    raise TestError
                return data

        server = MyServer(port=None, bind_address='tcp://127.0.0.1')
        try:
            self.assertIsInstance(server.context, SecureContext)
            response = zmq_get(server.port, data='hello!')
            self.assertEqual(response, 'hello!')

            # Ignore the exception in the other thread:
            clientserver.raise_exception_in_thread =  lambda *args: None
            try:
                with self.assertRaises(TestError):
                    zmq_get(server.port, data='error')
            finally:
                clientserver.raise_exception_in_thread = raise_exception_in_thread
        finally:
            server.shutdown()

    def test_raw_server(self):
        class MyServer(ZMQServer):
            def handler(self, data):
                if data == b'error':
                    raise TestError
                return data

        for argname in ["dtype", "type", "positional"]:
            if argname == 'dtype':
                server = MyServer(port=None, dtype='raw',
                                  bind_address='tcp://127.0.0.1')
            elif argname == 'type':
                server = MyServer(port=None, type='raw',
                                  bind_address='tcp://127.0.0.1')
            elif argname == 'positional':
                server = MyServer(None, 'raw',
                                  bind_address='tcp://127.0.0.1')
            try:
                self.assertIsInstance(server.context, SecureContext)
                response = zmq_get_raw(server.port, data=b'hello!')
                self.assertEqual(response, b'hello!')

                # Ignore the exception in the other thread:
                clientserver.raise_exception_in_thread =  lambda *args: None
                try:
                    self.assertIn(b'TestError', zmq_get_raw(server.port, data=b'error'))
                finally:
                    clientserver.raise_exception_in_thread = \
                        raise_exception_in_thread
            finally:
                server.shutdown()

    def test_pull_server(self):

        testcase = self
        got_data = threading.Event()

        class MyPullServer(ZMQServer):
            def handler(self, data):
                if data == 'error!':
                    return "not None!"
                testcase.assertEqual(data, 'hello!')
                got_data.set()

        server = MyPullServer(port=None, bind_address='tcp://127.0.0.1',
                              pull_only=True)

        # So we can catch errors raised by raise_exception_in_thread

        try:
            self.assertIsInstance(server.context, SecureContext)
            response = zmq_push(server.port, data='hello!')
            self.assertEqual(response, None)
            self.assertEqual(got_data.wait(timeout=1), True)
            got_data.clear()

            # Confirm you get an error when the handler returns something:
            got_error = threading.Event()
            class MockThread(object):
                def __init__(self, target, args):
                    self.target = target
                    self.args = args
                def start(self):
                    try:
                        self.target(*self.args)
                    except ValueError:
                        got_error.set()

            orig_thread = threading.Thread
            try:
                threading.Thread = MockThread
                response = zmq_push(server.port, data='error!')
                self.assertEqual(got_error.wait(timeout=1), True)
            finally:
                threading.Thread = orig_thread

            # Confirm the server still works:
            response = zmq_push(server.port, data='hello!')
            self.assertEqual(response, None)
            self.assertEqual(got_data.wait(timeout=1), True)
            got_data.clear()
        finally:
            server.shutdown()

    def test_customauth_backcompat(self):
        class MyCustomAuthServer(ZMQServer):
            def setup_auth(self, context):
                pass

        server = MyCustomAuthServer(port=None, bind_address='tcp://127.0.0.1')
        try:
            self.assertNotIsInstance(server.context, SecureContext)
        finally:
            server.shutdown()

DAEMON_CODE = """
import os, sys, json, time
started, finished = sys.argv[1:]
info = {'pid': os.getpid(), 'cwd': os.getcwd()}
if hasattr(os, 'getsid'):
    info['sid'] = os.getsid(0)
with open(started, 'w') as f:
    json.dump(info, f)
time.sleep(10)
open(finished, 'w').close()
"""

# On macOS, the Tcl/Tk bundled with Python 3.12 and 3.13 makes a forked child of a
# process that has imported tkinter fail if it forks again, so the launcher imports it.
LAUNCHER_CODE = """
import sys
sys.path.insert(0, sys.argv[1])
try:
    import tkinter
except ImportError:
    pass
from zprocess import start_daemon
start_daemon([sys.executable, '-c', sys.argv[2], sys.argv[3], sys.argv[4]])
"""


class StartDaemonTests(unittest.TestCase):
    def test_start_daemon(self):
        with tempfile.TemporaryDirectory() as tmp:
            started = os.path.join(tmp, 'started.json')
            finished = os.path.join(tmp, 'finished')
            # The launcher's output is on pipes, so communicate() returning before the
            # daemon finishes shows that the daemon does not hold them open.
            launcher = subprocess.Popen(
                [
                    sys.executable,
                    '-c',
                    LAUNCHER_CODE,
                    str(PROJECT_ROOT),
                    DAEMON_CODE,
                    started,
                    finished,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            _, stderr = launcher.communicate(timeout=30)
            self.assertEqual(launcher.returncode, 0, stderr.decode())
            self.assertFalse(os.path.exists(finished))
            deadline = time.monotonic() + 60
            while not os.path.exists(finished) and time.monotonic() < deadline:
                time.sleep(0.1)
            self.assertTrue(os.path.exists(finished), 'daemon did not run to completion')
            with open(started) as f:
                info = json.load(f)
        self.assertEqual(
            os.path.realpath(info['cwd']), os.path.realpath(tempfile.gettempdir())
        )
        if os.name == 'posix':
            self.assertNotEqual(info['sid'], os.getsid(0))


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
