#####################################################################
#                                                                   #
# clientserver.py                                                   #
#                                                                   #
# Copyright 2013 - 2018, Chris Billington                           #
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
import threading
import time
import traceback
from functools import partial
from socket import gethostbyname

import zmq

_path, _cwd = os.path.split(os.getcwd())
if _cwd == 'zprocess' and _path not in sys.path:
    # Running from within zprocess dir? Add to sys.path for testing during
    # development:
    sys.path.insert(0, _path)

import zprocess
from zprocess.security import SecureContext
from zprocess.utils import raise_exception_in_thread, TimeoutError

PY2 = sys.version_info[0] == 2
if PY2:
    str = unicode


def _typecheck_or_convert_data(data, dtype):
    """Utility function to check that messages are the valid type to be sent,
    for the send_type (one of 'pyobj', 'multipart', 'string', or 'raw').
    Returns converted data or raises TypeError. Only conversion done is to
    wrap single bytes objects into a single-element list for multipart
    messages. We *do not* do auto encoding of strings here. Strings can't be
    sent by raw and multipart sends, so yes, they need to be encoded, but we
    can't to auto *decoding* on the other end, because the data may not
    represent text - it might just be bytes. So we prefer symmetry and so
    don't encode here."""
    # when not using python objects, a null message should be an empty string:
    if data is None and dtype in ['raw', 'multipart']:
        data = b''
    if dtype == 'multipart' and isinstance(data, bytes):
        # Wrap up a single string into a list so it doesn't get sent
        # as one character per message!
        data = [data]
    # Type error checking:
    if dtype == 'raw':
        if not isinstance(data, bytes):
            msg = 'raw sockets can only send bytes, not {}.'.format(type(data))
            raise TypeError(msg)
    elif dtype == 'string':
        if PY2 and isinstance(data, bytes):
            # Auto convert assuming UTF8:
            data = data.decode('utf8')
        if not isinstance(data, str):
            msg = ('string sockets can only send strings, ' +
                   'not {}.'.format(type(data)))
            raise TypeError(msg)
    elif dtype == 'multipart':
        if not all(isinstance(part, bytes) for part in data):
            msg = ('multipart sockets can only send an iterable of ' 
                   'bytes objects, not {}.'.format(type(data)))
            raise TypeError(msg)
    elif dtype != 'pyobj':
        msg = ("invalid dtype %s, " % str(dtype) + 
               "must be 'raw', 'string', 'multipart' or 'pyobj'")
        raise ValueError(msg)
    return data


class ZMQServer(object):
    """Wrapper around a zmq.REP or zmq.PULL socket"""
    def __init__(self, port, dtype='pyobj', pull_only=False, 
                 bind_address='tcp://127.0.0.1', shared_secret=None,
                 allow_insecure=False):
        self.port = port
        self.dtype = dtype
        self.pull_only = pull_only
        self.bind_address = bind_address

        if 'setup_auth' in self.__class__.__dict__:
            # Backward compatibility for subclasses implementing their own
            # authentication:
            self.context = zmq.Context()
            self.auth = self.setup_auth(self.context)
            if self.pull_only:
                self.sock = self.context.socket(zmq.PULL)
            else:
                self.sock = self.context.socket(zmq.REP)
        else:
            # Our shared secret authentication:
            self.context = SecureContext(shared_secret=shared_secret)
            if self.pull_only:
                self.sock = self.context.socket(zmq.PULL,
                                                allow_insecure=allow_insecure)
            else:
                self.sock = self.context.socket(zmq.REP,
                                                allow_insecure=allow_insecure)

        self.sock.setsockopt(zmq.LINGER, 0)

        self.sock.bind('%s:%s' % (str(self.bind_address), str(self.port)))

        if self.dtype == 'raw':
            self.send = self.sock.send
            self.recv = self.sock.recv
        elif self.dtype == 'string':
            self.send = self.sock.send_string
            self.recv = self.sock.recv_string
        elif self.dtype == 'multipart':
            self.send = self.sock.send_multipart
            self.recv = self.sock.recv_multipart
        elif self.dtype == 'pyobj':
            self.send = partial(self.sock.send_pyobj,
                                protocol=zprocess.PICKLE_PROTOCOL)
            self.recv = self.sock.recv_pyobj
        else:
            msg = ("invalid dtype %s, must be 'raw', 'string', " +
                   "'multipart' or 'pyobj'" % str(self.dtype))
            raise ValueError(msg)
            
        self.mainloop_thread = threading.Thread(target=self.mainloop)
        self.mainloop_thread.daemon = True
        self.mainloop_thread.start()

    def setup_auth(self, context):
        """Deprecated. To be overridden by subclasses setting up their
        own authentication. If present in a subclass, this will be called
        and no shared secret authentication will be used."""
        pass

    def shutdown_on_interrupt(self):
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            sys.stderr.write('Interrupted, shutting down\n')
        finally:
            self.shutdown()
            
    def mainloop(self):
        while True:
            try:
                request_data = self.recv()
            except zmq.ContextTerminated:
                self.sock.close(linger=0)
                return
            try:
                response_data = self.handler(request_data)
                if self.pull_only and response_data is not None:
                    msg = ("Pull-only server hander() method returned " +
                           "non-None value %s. Ignoring." % str(response_data))
                    raise ValueError(msg)
                response_data = _typecheck_or_convert_data(response_data,
                                                           self.dtype)
            except Exception:
                # Raise the exception in a separate thread so that the
                # server keeps running:
                exc_info = sys.exc_info()
                raise_exception_in_thread(exc_info)
                exception_string = traceback.format_exc()
                if not self.pull_only:
                    # Send the error to the client:
                    msg = ("The server had an unhandled exception whilst " + 
                           "processing the request:\n%s" % str(exception_string))
                    response_data = zmq.ZMQError(msg)
                    if self.dtype == 'raw':
                        response_data = str(response_data).encode('utf8')
                    elif self.dtype == 'multipart':
                        response_data = [str(response_data).encode('utf8')]
                    elif self.dtype == 'string':
                        response_data = str(response_data)
                    response_data = _typecheck_or_convert_data(response_data,
                                                               self.dtype)
            if not self.pull_only:
                self.send(response_data)

    def shutdown(self):
        self.context.term()
        self.mainloop_thread.join()

    def handler(self, request_data):
        """To be overridden by subclasses. This is an example
        implementation"""
        response = ('This is an example ZMQServer. ' +
                    'Your request was %s.' % str(request_data))
        return response


class _Sender(object):
    """Wrapper around a zmq.PUSH or zmq.REQ socket, returning a callable
    for sending (and optionally receiving data)"""
    def __init__(self, dtype='pyobj', push_only=False,
                 shared_secret=None, allow_insecure=False):
        self.local = threading.local()
        self.dtype = dtype
        self.push_only = push_only
        self.shared_secret = shared_secret
        self.allow_insecure = allow_insecure

    def new_socket(self, host, port):
        # Every time the REQ/REP cadence is broken, we need to create
        # and bind a new socket to get it back on track. Also, we have
        # a separate socket for each thread. Also a new socket if there
        # is a different host or port.
        self.local.host = gethostbyname(host)
        self.local.port = int(port)
        context = SecureContext.instance(shared_secret=self.shared_secret)
        if self.push_only:
            self.local.sock = context.socket(zmq.PUSH,
                                             allow_insecure=self.allow_insecure)
        else:
            self.local.sock = context.socket(zmq.REQ,
                                             allow_insecure=self.allow_insecure)
        self.local.sock.setsockopt(zmq.LINGER, 0)
        self.local.sock.connect('tcp://%s:%d' % (self.local.host, self.local.port))
        # Different send/recv methods depending on the desired protocol:
        if self.dtype == 'raw':
            self.local.send = self.local.sock.send
            self.local.recv = self.local.sock.recv
        elif self.dtype == 'string':
            self.local.send = self.local.sock.send_string
            self.local.recv = self.local.sock.recv_string
        elif self.dtype == 'multipart':
            self.local.send = self.local.sock.send_multipart
            self.local.recv = self.local.sock.recv_multipart
        elif self.dtype == 'pyobj':
            self.local.send = partial(self.local.sock.send_pyobj,
                                      protocol=zprocess.PICKLE_PROTOCOL)
            self.local.recv = self.local.sock.recv_pyobj
        else:
            msg = ("invalid dtype %s, must be 'raw', 'string', " +
                   "'multipart' or 'pyobj'" % str(self.dtype))
            raise ValueError(msg)

    def __call__(self, port, host='127.0.0.1', data=None, timeout=5):
        """If self.push_only, send data on the push socket, ignoring timeout.
        Otherwise, uses reliable request-reply to send data to a zmq REP
        socket, and return the reply"""
        # We cache the socket so as to not exhaust ourselves of tcp
        # ports. However if a different server is in use, we need a new
        # socket. Also if we don't have a socket, we also need a new one:
        if (not hasattr(self.local, 'sock')
                or gethostbyname(host) != self.local.host
                or int(port) != self.local.port):
            self.new_socket(host, port)
        data = _typecheck_or_convert_data(data, self.dtype)
        try:
            self.local.send(data, zmq.NOBLOCK)
            if self.push_only:
                return
            events = self.local.sock.poll(timeout * 1000, flags=zmq.POLLIN)
            if events:
                response = self.local.recv()
            else:
                # The server hasn't replied. We don't know what it's doing, so
                # we'd better stop using this socket in case late messages
                # arrive on it in the future:
                raise TimeoutError('No response from server: timed out')
            if isinstance(response, Exception):
                raise response
            else:
                return response
        except:
            # Any exceptions, we want to stop using this socket:
            del self.local.sock
            raise


class ZMQClient(object):
    def __init__(self, shared_secret=None, allow_insecure=False):
        self.shared_secret = shared_secret
        self.allow_insecure = allow_insecure
        kwargs = {'shared_secret': shared_secret, 
                  'allow_insecure': allow_insecure}

        self.get = _Sender('pyobj', **kwargs)
        self.get_multipart = _Sender('multipart', **kwargs)
        self.get_string = _Sender('string', **kwargs)
        self.get_raw = _Sender('raw', **kwargs)
        self.push = _Sender('pyobj', push_only=True, **kwargs)
        self.push_multipart = _Sender('multipart', push_only=True, **kwargs)
        self.push_raw = _Sender('raw', push_only=True, **kwargs)
        self.push_string = _Sender('string', push_only=True, **kwargs)



# Backwards compatability follows:

# Default to on all interfaces and allow insecure connections.
_ZMQServer = ZMQServer
class ZMQServer(_ZMQServer):
    """Wrapper around a zmq.REP or zmq.PULL socket"""
    def __init__(self, port, dtype='pyobj', pull_only=False, 
                 bind_address='tcp://0.0.0.0', shared_secret=None,
                 allow_insecure=True):
        _ZMQServer.__init__(self, port, dtype=dtype, pull_only=pull_only,
                            bind_address=bind_address,
                            shared_secret=shared_secret,
                            allow_insecure=allow_insecure)


# methods for a default insecure client
_default_client = ZMQClient(allow_insecure=True)

zmq_get = _default_client.get
zmq_get_multipart = _default_client.get_multipart
zmq_get_string = _default_client.get_string
zmq_get_raw = _default_client.get_raw
zmq_push = _default_client.push
zmq_push_multipart = _default_client.push_multipart
zmq_push_string = _default_client.push_string
zmq_push_raw = _default_client.push_raw


__all__ = ['ZMQServer', 'ZMQClient',
           'zmq_get', 'zmq_get_multipart', 'zmq_get_string', 'zmq_get_raw',
           'zmq_push', 'zmq_push_multipart', 'zmq_push_string', 'zmq_push_raw']

