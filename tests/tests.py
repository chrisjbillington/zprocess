#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import unicode_literals, print_function, division

import unittest

import sys
PY2 = sys.version_info.major == 2
import os
import time
import threading

import zmq

import xmlrunner

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

from zprocess import (ZMQServer, Process, TimeoutError, RichStreamHandler, rich_print,
                      raise_exception_in_thread, zmq_get, zmq_push, zmq_get_raw,
                      ExternalBroker)
import zprocess.clientserver as clientserver
from zprocess.clientserver import _typecheck_or_convert_data
from zprocess.process_tree import _default_process_tree, EventBroker
shared_secret = _default_process_tree.shared_secret
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
        self.assertEqual(self.redirection_sock.recv_multipart(),
                         [b'stdout', b'hello from echo\n'])

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


class HeartbeatTests(unittest.TestCase):
    def setUp(self):
        """Create a sock for output redirection and a zmq port to mock a heartbeat
        server"""
        import zmq
        context = SecureContext.instance(shared_secret=shared_secret)
        self.heartbeat_sock = context.socket(zmq.REP)
        heartbeat_port = self.heartbeat_sock.bind_to_random_port('tcp://127.0.0.1')

        class mock_heartbeat_server(object):
            port = heartbeat_port

        self.mock_heartbeat_server = mock_heartbeat_server

    def test_subproc_lives_with_heartbeats(self):
        _default_process_tree.heartbeat_server = self.mock_heartbeat_server
        self.process = HeartbeatClientTestProcess()
        self.process.start()
        for i in range(3):
            # Wait for a heartbeat request:
            self.assertTrue(self.heartbeat_sock.poll(3000))
            # Echo it back:
            self.heartbeat_sock.send(self.heartbeat_sock.recv())

    def test_subproc_dies_without_heartbeats(self):
        _default_process_tree.heartbeat_server = self.mock_heartbeat_server
        self.process = HeartbeatClientTestProcess()
        self.process.start()
        # Wait for a heartbeat request:
        self.assertTrue(self.heartbeat_sock.poll(3000))
        # Don't respond to it
        time.sleep(2)
        # Process should be dead:
        self.assertIsNot(self.process.child.poll(), None)

    def test_subproc_dies_on_incorrect_response(self):
        _default_process_tree.heartbeat_server = self.mock_heartbeat_server
        self.process = HeartbeatClientTestProcess()
        self.process.start()
        # Wait for a heartbeat request:
        self.assertTrue(self.heartbeat_sock.poll(3000))
        # Echo it back wrongly:
        self.heartbeat_sock.send(self.heartbeat_sock.recv() + b'wrong')
        time.sleep(1)
        # Process should be dead:
        self.assertIsNot(self.process.child.poll(), None)

    def test_subproc_survives_until_kill_lock_released(self):
        _default_process_tree.heartbeat_server = self.mock_heartbeat_server
        self.process = HeartbeatClientTestProcess()
        to_child, from_child = self.process.start()
        # Wait for a heartbeat request:
        self.assertTrue(self.heartbeat_sock.poll(3000))
        # Tell child to acquire kill lock for 3 sec:
        to_child.put(None)
        # Don't respond to the heartbeat, process should still be alive 2 sec later
        time.sleep(2)
        # Process should be alive:
        self.assertIs(self.process.child.poll(), None)
        # After kill lock released, child should be terminated:
        time.sleep(2)
        # Process should be dead:
        self.assertIsNot(self.process.child.poll(), None)

    def test_parent_correctly_responds_to_heartbeats(self):
        # No mock server this time, we're testing the real one:
        _default_process_tree.heartbeat_server = None
        self.process = HeartbeatServerTestProcess()
        to_child, from_child = self.process.start()
        to_child.put(_default_process_tree.heartbeat_server.port)
        self.assertTrue(from_child.sock.poll(1000))
        self.assertTrue(from_child.get())

    def tearDown(self):
        self.heartbeat_sock.close()
        try:
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



class EventTests(unittest.TestCase):
    def test_events(self):
        proc = TestEventProcess()
        event = _default_process_tree.event('hello', role='wait')
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
        event = _default_process_tree.event('hello', role='wait', external_broker=broker_details)
        proc.start(broker_details)
        try:
            data = event.wait('1', timeout=1)
            self.assertEqual(data, u'boo')
        finally:
            proc.terminate()


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


class ClientServerTests(unittest.TestCase):

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

if __name__ == '__main__':
    output = 'test-reports'
    if PY2:
        output = output.encode('utf8')
    testRunner = xmlrunner.XMLTestRunner(output=output, verbosity=3)
    unittest.main(verbosity=3, testRunner=testRunner, exit=not sys.flags.interactive)
