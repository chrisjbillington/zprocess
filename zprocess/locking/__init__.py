#####################################################################
#                                                                   #
# __init__.py                                                       #
#                                                                   #
# Copyright 2013, Chris Billington                                  #
#                                                                   #
# This file is part of the zprocess project (see                    #
# https://bitbucket.org/cbillington/zprocess) and is licensed under #
# the Simplified BSD License. See the license.txt file in the root  #
# of the project for the full license.                              #
#                                                                   #
#####################################################################

from __future__ import division, unicode_literals, print_function, absolute_import

import sys
import os
import socket
import threading
import time
import weakref

PY2 = sys.version_info.major == 2
if PY2:
    str = unicode
import zmq

DEFAULT_TIMEOUT = 30  # seconds
DEFAULT_PORT = 7339

process_identifier_prefix = ''
thread_identifier_prefix = threading.local()

_zmq_lock_client = None


def _name_change_checks():
    if _zmq_lock_client is not None:
        # Clear thread local data so that the client id is re-generated in all threads:
        _zmq_lock_client.local = threading.local()


def set_client_process_name(name):
    global process_identifier_prefix
    _name_change_checks()
    process_identifier_prefix = name + '-'


def set_client_thread_name(name):
    _name_change_checks()
    thread_identifier_prefix.prefix = name + '-'


def get_client_id():
    try:
        prefix = thread_identifier_prefix.prefix
    except AttributeError:
        prefix = thread_identifier_prefix.prefix = ''
    thread_identifier = prefix + threading.current_thread().name
    process_identifier = process_identifier_prefix + str(os.getpid())
    host_name = socket.gethostname()
    return ':'.join([host_name, process_identifier, thread_identifier])


def _ensure_bytes(s):
    if isinstance(s, str):
        s = s.encode('utf8')
    elif not isinstance(s, bytes):
        raise TypeError("Bytes or string required, not %s" % str(type(s)))
    return s


class ZMQLockClient(object):

    RESPONSE_TIMEOUT = 5000

    def __init__(self, host, port):
        self.host = socket.gethostbyname(host)
        self.port = port
        self.lock = threading.Lock()
        # We'll store one zmq socket/poller for each thread, with thread
        # local storage:
        self.local = threading.local()

    def new_socket(self):
        # Every time the REQ/REP cadence is broken, we need to create
        # and bind a new socket to get it back on track. Also, we have
        # a separate socket for each thread:
        context = zmq.Context.instance()
        self.local.sock = context.socket(zmq.REQ)
        self.local.sock.setsockopt(zmq.LINGER, 0)
        self.local.poller = zmq.Poller()
        self.local.poller.register(self.local.sock, zmq.POLLIN)
        self.local.sock.connect('tcp://%s:%s' % (self.host, str(self.port)))
        self.local.client_id = _ensure_bytes(get_client_id())

    def say_hello(self, timeout=None):
        """Ping the server to test for a response"""
        try:
            if timeout is None:
                timeout = self.RESPONSE_TIMEOUT
            else:
                timeout = 1000 * timeout  # convert to ms
            if not hasattr(self.local, 'sock'):
                self.new_socket()
            start_time = time.time()
            self.local.sock.send(b'hello', zmq.NOBLOCK)
            events = self.local.poller.poll(timeout)
            if events:
                response = self.local.sock.recv().decode('utf8')
                if response == 'hello':
                    return round((time.time() - start_time) * 1000, 2)
                raise zmq.ZMQError('Invalid repsonse from server: ' + response)
            raise zmq.ZMQError('No response from zlock server: timed out')
        except:
            self.local.sock.close(linger=False)
            del self.local.sock
            raise

    def get_protocol_version(self, timeout=None):
        """Ask the server what protocol version it is running"""
        try:
            if timeout is None:
                timeout = self.RESPONSE_TIMEOUT
            else:
                timeout = 1000 * timeout  # convert to ms
            if not hasattr(self.local, 'sock'):
                self.new_socket()
            self.local.sock.send(b'protocol', zmq.NOBLOCK)
            events = self.local.poller.poll(timeout)
            if events:
                response = self.local.sock.recv().decode('utf8')
                if 'KeyError' in response:
                    # This is what zlock used to say before we gave it the 'protocol'
                    # method. That means it's version 1.0.0
                    return '1.0.0'
                else:
                    return response
            raise zmq.ZMQError('No response from zlock server: timed out')
        except:
            self.local.sock.close(linger=False)
            del self.local.sock
            raise

    def acquire(self, key, timeout=None, read_only=False):
        if timeout is None:
            timeout = DEFAULT_TIMEOUT
        timeout = str(timeout).encode('utf8')
        if not hasattr(self.local, 'sock'):
            self.new_socket()
        key = _ensure_bytes(key)
        messages = [b'acquire', key, self.local.client_id, timeout]
        if read_only:
            messages.append(b'read_only')
        try:
            while True:
                self.local.sock.send_multipart(messages, zmq.NOBLOCK)
                events = self.local.poller.poll(self.RESPONSE_TIMEOUT)
                if not events:
                    raise zmq.ZMQError('No response from zlock server: timed out')
                response = self.local.sock.recv().decode('utf8')
                if response == 'ok':
                    break
                elif response == 'retry':
                    continue
                elif 'acquire() takes exactly 4 arguments (5 given)' in response:
                    # Zlock version too old to support read_only.
                    if read_only:
                        msg = "Client requested a read-only lock, but the zlock server "
                        msg += "is too old to support this. Please update the zlock "
                        msg += "server to zprocess >= 2.7.0 for read-only locks."
                        raise zmq.ZMQError(msg)
                raise zmq.ZMQError(response)
        except:
            if hasattr(self.local, 'sock'):
                self.local.sock.close(linger=False)
                del self.local.sock
            raise

    def release(self, key, client_id):
        if not hasattr(self.local, 'sock'):
            self.new_socket()
        try:
            if client_id is None:
                client_id = self.local.client_id
            key = _ensure_bytes(key)
            self.local.sock.send_multipart([b'release', key, client_id])
            events = self.local.poller.poll(self.RESPONSE_TIMEOUT)
            if not events:
                raise zmq.ZMQError('No response from zlock server: timed out')
            response = self.local.sock.recv().decode('utf8')
            if response == 'ok':
                return
            raise zmq.ZMQError(response)
        except:
            if hasattr(self.local, 'sock'):
                self.local.sock.close(linger=False)
                del self.local.sock
            raise


class Lock(object):
    def __init__(self, key, read_only=False):
        self.key = key
        self.read_only = read_only

    def acquire(self, timeout=None, read_only=None):
        if read_only is None:
            read_only = self.read_only
        acquire(self.key, timeout, read_only)

    def release(self):
        release(self.key)

    def __enter__(self):
        self.acquire()

    def __exit__(self, type, value, traceback):
        self.release()


def acquire(key, timeout=None, read_only=False):
    """Acquire a lock identified by key, for a specified time in
    seconds. Blocks until success, raises exception if the server isn't
    responding"""
    if _zmq_lock_client is None:
        raise RuntimeError('Not connected to a zlock server')
    _zmq_lock_client.acquire(key, timeout, read_only)


def release(key, client_id=None):
    """Release the lock identified by key. Raises an exception if the
    lock was not held, or was held by someone else, or if the server
    isn't responding. If client_id is provided, one thread can release
    the lock on behalf of another, but this should not be the normal
    usage."""
    if _zmq_lock_client is None:
        raise RuntimeError('Not connected to a zlock server')
    _zmq_lock_client.release(key, client_id)


def ping(timeout=None):
    if _zmq_lock_client is None:
        raise RuntimeError('Not connected to a zlock server')
    return _zmq_lock_client.say_hello(timeout)


def get_protocol_version(timeout=None):
    if _zmq_lock_client is None:
        raise RuntimeError('Not connected to a zlock server')
    return _zmq_lock_client.get_protocol_version(timeout)


def set_default_timeout(t):
    """Sets how long the locks should be acquired for before the server
    times them out and allows other clients to acquire them. Attempting
    to release them will then result in an exception."""
    global DEFAULT_TIMEOUT
    DEFAULT_TIMEOUT = t


def connect(host='localhost', port=DEFAULT_PORT, timeout=None):
    """This method should be called at program startup, it establishes
    communication with the server and ensures it is responding"""
    global _zmq_lock_client
    _zmq_lock_client = ZMQLockClient(host, port)
    # We ping twice since the first does initialisation and so takes
    # longer. The second will be more accurate:
    ping(timeout)
    return ping(timeout)


if __name__ == '__main__':
    # test:
    connect()
    with Lock('test', read_only=True):
        print('with lock')
