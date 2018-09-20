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
from textwrap import dedent
from logging import Handler

PY2 = sys.version_info.major == 2
if PY2:
    str = unicode
import zmq

DEFAULT_PORT = 7340

_zmq_log_client = None


class ZMQLogClient(object):

    RESPONSE_TIMEOUT = 5000

    def __init__(self, host, port):
        self.host = socket.gethostbyname(host)
        self.port = port
        # We'll store one zmq socket for each thread, with thread local storage:
        self.local = threading.local()

    def new_push_socket(self):
        # We have a separate push socket for each thread that needs to push:
        context = zmq.Context.instance()
        self.local.push_sock = context.socket(zmq.PUSH)
        self.local.push_sock.setsockopt(zmq.LINGER, 0)
        self.local.push_sock.set_hwm(1000)
        self.local.push_sock.connect('tcp://%s:%s' % (self.host, str(self.port)))

    def new_req_socket(self):
        # We have a separate req socket for each thread that needs one:
        context = zmq.Context.instance()
        self.local.req_sock = context.socket(zmq.REQ)
        self.local.req_sock.setsockopt(zmq.LINGER, 0)
        self.local.req_sock.connect('tcp://%s:%s' % (self.host, str(self.port)))
        self.local.poller = zmq.Poller()
        self.local.poller.register(self.local.req_sock, zmq.POLLIN)

    def say_hello(self, timeout=None):
        """Ping the server for a response."""
        try:
            if timeout is None:
                timeout = self.RESPONSE_TIMEOUT
            else:
                timeout = 1000 * timeout  # convert to ms
            if not hasattr(self.local, 'req_sock'):
                self.new_req_socket()
            start_time = time.time()
            self.local.req_sock.send(b'hello', zmq.NOBLOCK)
            events = self.local.poller.poll(timeout)
            if events:
                response = self.local.req_sock.recv().decode('utf8')
                if response == 'hello':
                    return round((time.time() - start_time) * 1000, 2)
                raise zmq.ZMQError('Invalid response from server: ' + response)
            raise zmq.ZMQError('No response from zlog server: timed out')
        except:
            self.local.req_sock.close(linger=False)
            del self.local.req_sock
            del self.local.poller
            raise

    def log(self, client_id, filepath, message):
        """Send a message to the logging server, asking it to write it to the specified
        filepath"""
        if not hasattr(self.local, 'push_sock'):
            self.new_push_socket()
        if not isinstance(filepath, bytes):
            filepath = filepath.encode('utf8')
        if not isinstance(message, bytes):
            message = message.encode('utf8')
        messages = [b'log', client_id, filepath, message]
        try:
            self.local.new_push_socket.send_multipart(messages, zmq.NOBLOCK)
        except zmq.Again:
            msg = """Warning: zlog server not receiving log messages. Logging may not be
                functional"""
            sys.stderr.write(dedent(msg))

    def close(self, client_id, filepath, timeout=None):
        """Tell the server a client is done with the file"""
        try:
            if timeout is None:
                timeout = self.RESPONSE_TIMEOUT
            else:
                timeout = 1000 * timeout  # convert to ms
            if not hasattr(self.local, 'req_sock'):
                self.new_req_socket()
            start_time = time.time()
            if not isinstance(filepath, bytes):
                filepath = filepath.encode('utf8')
            messages = [b'close', client_id, filepath]
            self.local.req_sock.send_multipart(messages, zmq.NOBLOCK)
            events = self.local.poller.poll(timeout)
            if events:
                response = self.local.req_sock.recv().decode('utf8')
                if response == 'ok':
                    return round((time.time() - start_time) * 1000, 2)
                raise zmq.ZMQError('Invalid response from server: ' + response)
            raise zmq.ZMQError('No response from zlog server: timed out')
        except:
            self.local.req_sock.close(linger=False)
            del self.local.req_sock
            del self.local.poller
            raise


class ZMQLoggingHandler(Handler):
    """Logging handler that formats and sends log messages to a zlog server"""

    def __init__(self, filename):
        self.filename = os.path.abspath(filename)
        # A unique ID so that the server can identify us:
        self.client_id = os.urandom(32)
        Handler.__init__(self, filename)

    def close(self):
        """Tell the server we're done with the file. It will know to close the file once
        all clients are done with it."""
        Handler.close(self)
        if _zmq_log_client is None:
            raise RuntimeError('Not connected to a zlog server')
        _zmq_log_client.close(self.client_id, self.filename)

    def emit(self, record):
        """Format and send the record to the server"""
        msg = self.format(record) + '\n'
        if _zmq_log_client is None:
            raise RuntimeError('Not connected to a zlog server')
        _zmq_log_client.log(self.client_id, self.filename, msg)


def ping(timeout=None):
    if _zmq_log_client is None:
        raise RuntimeError('Not connected to a zlog server')
    return _zmq_log_client.say_hello(timeout)


def connect(host='localhost', port=DEFAULT_PORT, timeout=None):
    """This method should be called at program startup, it establishes
    communication with the server and ensures it is responding"""
    global _zmq_log_client
    _zmq_log_client = ZMQLogClient(host, port)
    # We ping twice since the first does initialisation and so takes
    # longer. The second will be more accurate:
    ping(timeout)
    return ping(timeout)


if __name__ == '__main__':
    connect()
