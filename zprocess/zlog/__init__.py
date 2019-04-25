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
from logging import Handler
import uuid

try:
    import builtins
except ImportError:
    import __builtin__ as builtins

PY2 = sys.version_info.major == 2
if PY2:
    str = unicode
import zmq
from zprocess.security import SecureContext
DEFAULT_PORT = 7340

_zmq_log_client = None


def dedent(s):
    return ' '.join(s.split())


class ZLogClient(object):

    RESPONSE_TIMEOUT = 5000

    def __init__(self, host, port, shared_secret=None, allow_insecure=True):
        self.host = socket.gethostbyname(host)
        self.port = port
        self.shared_secret = shared_secret
        self.allow_insecure = allow_insecure
        # We'll store one zmq socket for each thread, with thread local storage:
        self.local = threading.local()
        self.suppress_further_warnings = False

    def _new_socket(self):
        # We have a separate push socket for each thread that needs to push:
        context = SecureContext.instance(shared_secret=self.shared_secret)
        self.local.sock = context.socket(zmq.DEALER, allow_insecure=self.allow_insecure)
        self.local.sock.setsockopt(zmq.LINGER, 0)
        self.local.sock.set_hwm(1000)
        self.local.sock.connect(
            'tcp://%s:%s' % (self.host, str(self.port)), timeout=self.RESPONSE_TIMEOUT
        )
        self.local.poller = zmq.Poller()
        self.local.poller.register(self.local.sock, zmq.POLLIN)

    def handler(self, filepath):
        """Return a logging handler configured to communicate with this server"""
        return ZMQLoggingHandler(self, filepath)

    def _send(self, *messages):
        if not hasattr(self.local, 'sock'):
            self._new_socket()
        self.local.sock.send_multipart([b''] + list(messages), zmq.NOBLOCK)

    def _recv(self, timeout=None):
        try:
            if timeout is None:
                timeout = self.RESPONSE_TIMEOUT
            else:
                timeout = 1000 * timeout  # convert to ms
            events = self.local.poller.poll(timeout)
            if not events:
                raise zmq.ZMQError('No response from zlog server: timed out')
            response = self.local.sock.recv_multipart()
            if len(response) != 2 or response[0] != b'':
                raise zmq.ZMQError('Malformed message from server: ' + str(response))
            if response[1].startswith(b'error:'):
                raise ValueError(response[1].decode('utf8'))
            return response[1]
        except:
            self.local.sock.close(linger=False)
            del self.local.sock
            del self.local.poller
            raise

    def ping(self, timeout=None):
        """Ping the server for a response."""
        start_time = time.time()
        self._send(b'hello')
        response = self._recv(timeout).decode('utf8')
        if response == 'hello':
            return round((time.time() - start_time) * 1000, 2)
        raise zmq.ZMQError('Invalid response from server: ' + response)

    def get_protocol_version(self, timeout=None):
        """Ask the server what protocol version it is running"""
        self._send(b'protocol')
        return self._recv(timeout).decode('utf8')

    def check_access(self, filepath, timeout=None):
        """Send a message to the logging server, asking it to check that it can open the
        log file in append mode. Raises an exception if the file cannot be opened."""
        if not isinstance(filepath, bytes):
            filepath = filepath.encode('utf8')
        self._send(b'check_access', filepath)
        response = self._recv(timeout).decode('utf8')
        if response == 'ok':
            return
        # Raise the exception returned by the server:
        try:
            exc_class_name, message = response.split(': ', 1)
            exc_class = getattr(builtins, exc_class_name, OSError)
        except ValueError:
            exc_class = OSError
            message = response
        if not issubclass(exc_class, OSError):
            exc_class = OSError
        raise exc_class(message)

    def log(self, client_id, filepath, message):
        """Send a message to the logging server, asking it to write it to the specified
        filepath"""
        if not isinstance(filepath, bytes):
            filepath = filepath.encode('utf8')
        if not isinstance(message, bytes):
            message = message.encode('utf8')
        try:
            self._send(b'log', client_id, filepath, message)
        except zmq.Again:
            if not self.suppress_further_warnings:
                self.suppress_further_warnings = True
                msg = """Warning: zlog server not receiving log messages. Logging may
                    not be functional\n"""
                sys.stderr.write(dedent(msg))

    def done(self, client_id, filepath, timeout=None):
        """Tell the server a client is done with the file"""
        if not isinstance(filepath, bytes):
            filepath = filepath.encode('utf8')
        self._send(b'done', client_id, filepath)
        response = self._recv(timeout).decode('utf8')
        if response != 'ok':
            raise zmq.ZMQError('Invalid response from server: ' + response)


class ZMQLoggingHandler(Handler):
    """Logging handler that sends log messages to a zlog server"""

    def __init__(self, zlog_client, filepath):
        self.client = zlog_client
        self.filepath = os.path.abspath(filepath)
        # A unique ID so that the server can identify us:
        self.client_id = uuid.uuid4().hex.encode('utf8')
        Handler.__init__(self)
        self.client.check_access(self.filepath)

    def close(self):
        """Tell the server we're done with the file. It will know to close the file once
        all clients are done with it."""
        Handler.close(self)
        self.client.done(self.client_id, self.filepath)

    def emit(self, record):
        """Format and send the record to the server"""
        msg = self.format(record)
        self.client.log(self.client_id, self.filepath, msg)



# Backwards compatibility follots

_default_zlog_client = None

_ZMQLoggingHandler = ZMQLoggingHandler
"""Backward compatibility to allow instantiating a lock without a ZLockClient as the
first argument"""
class ZMQLoggingHandler(_ZMQLoggingHandler):
    def __init__(self, *args, **kwargs):
        if not args or not isinstance(args[0], ZLogClient):
            if _default_zlog_client is None:
                raise RuntimeError('Not connected to a zlog server')
            args = (_default_zlog_client,) + args
        _ZMQLoggingHandler.__init__(self, *args, **kwargs)


def ping(timeout=None):
    """Deprecated. Instantiate a ZLogClient and call its ping() method instead."""
    if _default_zlog_client is None:
        raise RuntimeError('Not connected to a zlog server')
    return _default_zlog_client.ping(timeout)


def connect(host='localhost', port=DEFAULT_PORT, timeout=None):
    """Deprecated. Instantiate a ZlogClient and call its ping() method to check
    connectivity instead"""
    global _default_zlog_client
    _default_zlog_client = ZLogClient(host, port)
    # We ping twice since the first does initialisation and so takes
    # longer. The second will be more accurate:
    _default_zlog_client.ping(timeout)
    return _default_zlog_client.ping(timeout)


def get_protocol_version(timeout=None):
    """Deprecated. Instantiate a ZLogClient and call its method instead"""
    if _default_zlog_client is None:
        raise RuntimeError('Not connected to a zlog server')
    return _default_zlog_client.get_protocol_version(timeout)


def check_access(filepath, timeout=None):
    """Deprecated. Instantiate a ZLogClient and call its method instead"""
    if _default_zlog_client is None:
        raise RuntimeError('Not connected to a zlog server')
    return _default_zlog_client.check_access(filepath, timeout)


if __name__ == '__main__':
    connect()
    print(get_protocol_version())
    import logging

    logger = logging.Logger('test')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(ZMQLoggingHandler('test.log'))
    while True:
        time.sleep(1)
        logger.info(str(time.time()) + ': this is a log message')
