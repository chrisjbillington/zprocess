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
from zprocess.security import SecureContext

DEFAULT_TIMEOUT = 30  # seconds
DEFAULT_PORT = 7339


def _ensure_bytes(s):
    if isinstance(s, str):
        s = s.encode('utf8')
    elif not isinstance(s, bytes):
        raise TypeError("Bytes or string required, not %s" % str(type(s)))
    return s


class ZLockClient(object):

    RESPONSE_TIMEOUT = 5000

    def __init__(
        self,
        host,
        port,
        shared_secret=None,
        allow_insecure=True,
        default_timeout=DEFAULT_TIMEOUT,
        process_name=''
    ):
        self.host = socket.gethostbyname(host)
        self.port = port
        self.shared_secret = shared_secret
        self.allow_insecure = allow_insecure
        # We'll store one zmq socket/poller for each thread, with thread
        # local storage:
        self.local = threading.local()
        self.default_timeout = default_timeout
        self.process_name = process_name
        self.thread_name = threading.local()

    def set_default_timeout(self, timeout):
        self.default_timeout = timeout

    def _new_socket(self, timeout=None):
        # Every time the REQ/REP cadence is broken, we need to create
        # and bind a new socket to get it back on track. Also, we have
        # a separate socket for each thread:
        if timeout is None:
            timeout = self.RESPONSE_TIMEOUT
        context = SecureContext.instance(shared_secret=self.shared_secret)
        self.local.sock = context.socket(zmq.REQ, allow_insecure=self.allow_insecure)
        try:
            self.local.sock.setsockopt(zmq.LINGER, 0)
            self.local.poller = zmq.Poller()
            self.local.poller.register(self.local.sock, zmq.POLLIN)
            self.local.sock.connect(
                'tcp://%s:%s' % (self.host, str(self.port)), timeout
            )
            self.local.client_id = _ensure_bytes(self._make_client_id())
        except:
            # Didn't work, don't keep it:
            del self.local.sock
            raise

    def lock(self, key, read_only=False):
        """return a Lock for exclusive access to a resource identified by the given
        key"""
        return Lock(self, key, read_only=read_only)

    def ping(self, timeout=None):
        """Ping the server to test for a response"""
        if timeout is None:
            timeout = self.RESPONSE_TIMEOUT
        else:
            timeout = 1000 * timeout  # convert to ms
        if not hasattr(self.local, 'sock'):
            self._new_socket(timeout)
        try:
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
        if timeout is None:
            timeout = self.RESPONSE_TIMEOUT
        else:
            timeout = 1000 * timeout  # convert to ms
        if not hasattr(self.local, 'sock'):
            self._new_socket(timeout)
        try:
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
            self._new_socket()
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
                    return self.local.client_id
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

    def release(self, key, client_id=None):
        if not hasattr(self.local, 'sock'):
            self._new_socket()
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

    def set_process_name(self, name):
        """Set a string used as a prefix to the process id in the client id used by
        locks with this client. This can be useful for debugging when reading the zlock
        server logs."""
        self.process_name = name
        # Regenerate client id
        self.local.client_id = _ensure_bytes(self._make_client_id())

    def set_thread_name(self, name):
        """Set a string used as a prefix to the thread id in the client id used by
        locks with this client. This can be useful for debugging when reading the zlock
        server logs."""
        self.thread_name.name = name
        # Regenerate client id
        self.local.client_id = _ensure_bytes(self._make_client_id())

    def _make_client_id(self):
        try:
            thread_name = self.thread_name.name
        except AttributeError:
            thread_name = self.thread_name.name = ''
        thread_prefix = thread_name + '-' if thread_name else ''
        process_prefix = self.process_name + '-' if self.process_name else '' 
        thread_identifier = thread_prefix + threading.current_thread().name
        process_identifier = process_prefix + str(os.getpid())
        host_name = socket.gethostname()
        return ':'.join([host_name, process_identifier, thread_identifier])


class Lock(object):
    def __init__(self, zlock_client, key, read_only=False):
        self.client = zlock_client
        self.key = key
        self.read_only = read_only
        self._client_id = None

    def acquire(self, timeout=None, read_only=None):
        if read_only is None:
            read_only = self.read_only
        # We save the client id used for acquisition so that we can still release the
        # lock if it the client id is changed whilst we're holding it.
        self._client_id = self.client.acquire(self.key, timeout, read_only)

    def release(self):
        self.client.release(self.key, self._client_id)

    def __enter__(self):
        self.acquire()

    def __exit__(self, type, value, traceback):
        self.release()


# Backwards compatability follows:

_default_zlock_client = None
_DEFAULT_TIMEOUT = None
_process_name = None

_Lock = Lock
"""Backward compatibility to allow instantiating a lock without a ZLockClient as the
first argument"""
class Lock(_Lock):
    def __init__(self, *args, **kwargs):
        if not args or not isinstance(args[0], ZLockClient):
            if _default_zlock_client is None:
                raise RuntimeError('Not connected to a zlock server')
            args = (_default_zlock_client,) + args
        _Lock.__init__(self, *args, **kwargs)


def connect(host='localhost', port=DEFAULT_PORT, timeout=None):
    """Deprecated. Instantiate a ZlockClient and call its ping() method to check
    connectivity instead"""
    global _default_zlock_client
    kwargs = {}
    if _DEFAULT_TIMEOUT is not None:
        kwargs['default_timeout'] = _DEFAULT_TIMEOUT
    if _process_name is not None:
        kwargs['process_name'] = _process_name
    _default_zlock_client = ZLockClient(host, port, **kwargs)
    # We ping twice since the first does initialisation and so takes
    # longer. The second will be more accurate:
    ping(timeout)
    return ping(timeout)


def ping(timeout=None):
    """Deprecated. Instantiate a ZLockClient and call its ping() method instead."""
    if _default_zlock_client is None:
        raise RuntimeError('Not connected to a zlock server')
    return _default_zlock_client.ping(timeout)


def get_protocol_version(timeout=None):
    """Deprecated. Instantiate a ZLockClient and call its method instead"""
    if _default_zlock_client is None:
        raise RuntimeError('Not connected to a zlock server')
    return _default_zlock_client.get_protocol_version(timeout)


def set_default_timeout(timeout):
    """Backward compatibility. Set the default timeout of the default global zlock
    client, or, if it's not created yet, sets a global variable that will be used when
    it is created."""
    global _DEFAULT_TIMEOUT
    if _default_zlock_client is not None:
        _default_zlock_client.default_timeout = timeout
    else:
        _DEFAULT_TIMEOUT = timeout


def set_client_process_name(name):
    """Deprecated. Instantiate a ZLockClient and call its method instead"""
    name += '-'
    global _process_name
    if _default_zlock_client is not None:
        _default_zlock_client.set_process_name(name)
    else:
        _process_name = name


def set_client_thread_name(name):
    """Deprecated. Instantiate a ZLockClient and call its method instead"""
    if _default_zlock_client is not None:
        raise RuntimeError("Can't set thread name before instantiating ZLogClient")
    _default_zlock_client.set_thread_name(name)


if __name__ == '__main__':
    # test:
    connect()
    with Lock('test', read_only=True):
        print('with lock')
