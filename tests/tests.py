#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import unicode_literals, print_function, division

import unittest

import sys
import os
import time

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

from zprocess import _typecheck_or_convert_data, ZMQServer, Process, TimeoutError, HeartbeatServer


class  TypeCheckConvertTests(unittest.TestCase):
    """test the _typecheck_or_convert_data function"""

    def test_turns_None_into_empty_bytestring_raw(self):
        from zprocess import _typecheck_or_convert_data
        result = _typecheck_or_convert_data(None, 'raw')
        self.assertEqual(result, b'')

    def test_turns_None_into_empty_bytestring_multipart(self):
        from zprocess import _typecheck_or_convert_data
        result = _typecheck_or_convert_data(None, 'multipart')
        self.assertEqual(result, [b''])

    def test_wraps_bytestring_into_list_multipart(self):
        from zprocess import _typecheck_or_convert_data
        data = b'spam'
        result = _typecheck_or_convert_data(data, 'multipart')
        self.assertEqual(result, [data])

    def test_accepts_bytes_raw(self):
        from zprocess import _typecheck_or_convert_data
        data = b'spam'
        result = _typecheck_or_convert_data(data, 'raw')
        self.assertEqual(result, data)

    def test_accepts_list_of_bytes_multipart(self):
        from zprocess import _typecheck_or_convert_data
        data = [b'spam', b'ham']
        result = _typecheck_or_convert_data(data, 'multipart')
        self.assertEqual(result, data)

    def test_accepts_string_string(self):
        from zprocess import _typecheck_or_convert_data
        data = 'spam'
        result = _typecheck_or_convert_data(data, 'string')
        self.assertEqual(result, data)

    def test_accepts_pyobj_pyobj(self):
        from zprocess import _typecheck_or_convert_data
        data = {'spam': ['ham'], 'eggs': True}
        result = _typecheck_or_convert_data(data, 'pyobj')
        self.assertEqual(result, data)

    def test_rejects_string_raw(self):
        from zprocess import _typecheck_or_convert_data
        data = 'spam'
        with self.assertRaises(TypeError):
            _typecheck_or_convert_data(data, 'raw')

    def test_rejects_string_multipart(self):
        from zprocess import _typecheck_or_convert_data
        data = [b'spam', 'ham']
        with self.assertRaises(TypeError):
            _typecheck_or_convert_data(data, 'multipart')

    def test_rejects_pyobj_string(self):
        from zprocess import _typecheck_or_convert_data
        data = {'spam': ['ham'], 'eggs': True}
        with self.assertRaises(TypeError):
            _typecheck_or_convert_data(data, 'string')

    def test_rejects_bytes_string(self):
        from zprocess import _typecheck_or_convert_data
        data = b'spam'
        with self.assertRaises(TypeError):
            _typecheck_or_convert_data(data, 'string')

    def test_rejects_invalid_send_type(self):
        data = {'spam': ['ham'], 'eggs': True}
        with self.assertRaises(ValueError):
            _typecheck_or_convert_data(data, 'invalid_send_type')


class TestProcess(Process):
    def run(self):
        import sys
        item = self.from_parent.get()
        x, y = item
        sys.stdout.write(repr(x))
        sys.stderr.write(y)
        self.to_parent.put(item)


class ProcessClassTests(unittest.TestCase):

    def setUp(self):
        """Create a subprocess with output redirection to a zmq port"""
        import zmq
        self.redirection_sock = zmq.Context.instance().socket(zmq.PULL)
        redirection_port = self.redirection_sock.bind_to_random_port('tcp://127.0.0.1')
        self.process = TestProcess(redirection_port)

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

        subproc_output = []

        while self.redirection_sock.poll(100):
            subproc_output.append(self.redirection_sock.recv_multipart())
        self.assertEqual(subproc_output, [[b'stdout', repr(x).encode('utf8')], [b'stderr', y.encode('utf8')]])

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
        import zmq
        sock = zmq.Context.instance().socket(zmq.REQ)
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
        """Create a sock for output redirection and a zmq port to mock a heartbeat server"""
        import zmq
        self.heartbeat_sock = zmq.Context.instance().socket(zmq.REP)
        heartbeat_port = self.heartbeat_sock.bind_to_random_port('tcp://127.0.0.1')

        class mock_heartbeat_server(object):
            port = heartbeat_port

        self.mock_heartbeat_server = mock_heartbeat_server

    def test_subproc_lives_with_heartbeats(self):
        HeartbeatServer.instance = self.mock_heartbeat_server
        self.process = HeartbeatClientTestProcess()
        self.process.start()
        for i in range(3):
            # Wait for a heartbeat request:
            self.assertTrue(self.heartbeat_sock.poll(3000))
            # Echo it back:
            self.heartbeat_sock.send(self.heartbeat_sock.recv())

    def test_subproc_dies_without_heartbeats(self):
        HeartbeatServer.instance = self.mock_heartbeat_server
        self.process = HeartbeatClientTestProcess()
        self.process.start()
        # Wait for a heartbeat request:
        self.assertTrue(self.heartbeat_sock.poll(3000))
        # Don't respond to it
        time.sleep(2)
        # Process should be dead:
        self.assertIsNot(self.process.child.poll(), None)

    def test_subproc_dies_on_incorrect_response(self):
        HeartbeatServer.instance = self.mock_heartbeat_server
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
        HeartbeatServer.instance = self.mock_heartbeat_server
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
        # Process should be alive:
        self.assertIsNot(self.process.child.poll(), None)

    def test_parent_correctly_responds_to_heartbeats(self):
        # No mock server this time, we're testing the real one:
        HeartbeatServer.instance = None
        self.process = HeartbeatServerTestProcess()
        to_child, from_child = self.process.start()
        to_child.put(HeartbeatServer.instance.port)
        self.assertTrue(from_child.sock.poll(1000))
        self.assertTrue(from_child.get())

    def tearDown(self):
        self.heartbeat_sock.close()
        try:
            self.process.terminate()
        except Exception:
            pass # already dead


if __name__ == '__main__':
    unittest.main(verbosity=3)

# TODO:
# ZMQServer, all four types, zmq_get all four types, zmq_push, all four types
# Process class
#   test an RPC call
#   with output redirection
#   Test unicode output
#
# subprocess_with_queues
#   with output redirection
# Test heartbeating
# How?
