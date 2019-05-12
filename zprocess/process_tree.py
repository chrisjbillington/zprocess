from __future__ import division, unicode_literals, print_function, absolute_import
import sys
PY2 = sys.version_info.major == 2
import os
import threading
import time
import signal
import weakref
import json
import ctypes
import atexit
from ctypes.util import find_library
from socket import gethostbyname
import logging

import zmq

_path, _cwd = os.path.split(os.getcwd())
if _cwd == 'zprocess' and _path not in sys.path:
    # Running from within zprocess dir? Add to sys.path for testing during
    # development:
    sys.path.insert(0, _path)

import ipaddress
import zprocess
from zprocess.security import SecureContext
from zprocess.utils import Interruptor, Interrupted, TimeoutError
from zprocess.remote import (
    RemoteProcessClient,
    RemoteOutputReceiver,
    DEFAULT_PORT as REMOTE_DEFAULT_PORT,
)
from zprocess.locking import ZLockClient, DEFAULT_PORT as ZLOCK_DEFAULT_PORT
from zprocess.zlog import ZLogClient, DEFAULT_PORT as ZLOG_DEFAULT_PORT

PROCESS_CLASS_WRAPPER = 'zprocess.process_class_wrapper'

PY2 = sys.version_info[0] == 2
if PY2:
    import cPickle as pickle
    from Queue import Queue, Empty
    import subprocess32 as subprocess
    from time import time as monotonic
    str = unicode
else:
    import pickle
    from queue import Queue, Empty
    import subprocess
    from time import monotonic


class HeartbeatServer(object):
    """A server which receives messages from clients and echoes them back. Each
    process has a HeartbeatServer to provide heartbeating to its subprocesses -
    there is not only one in the top process.
    """
    def __init__(self, bind_address='tcp://0.0.0.0', 
                 shared_secret=None):
        context = SecureContext.instance(shared_secret=shared_secret)
        self.sock = context.socket(zmq.REP)
        self.port = self.sock.bind_to_random_port(bind_address)
        self.mainloop_thread = threading.Thread(target=self.mainloop)
        self.mainloop_thread.daemon = True
        self.mainloop_thread.start()

    def mainloop(self):
        try:
            zmq.proxy(self.sock, self.sock)
        except zmq.ContextTerminated:
            # Shutting down:
            self.sock.close(linger=0)
            return


class HeartbeatClient(object):

    # How long between heartbeats?
    HEARTBEAT_INTERVAL = 1
    # How long without a heartbeat until we kill the process? Wait longer when the
    # heartbeat server is remote to be more forgiving of the network:
    LOCALHOST_TIMEOUT = 1
    REMOTE_TIMEOUT = 10

    """A heartbeating thread that terminates the process if it doesn't get the
    heartbeats back within one second, unless a lock is held."""
    def __init__(self, server_host, server_port,
                 shared_secret=None, allow_insecure=False):
        self.lock = threading.Lock()
        context = SecureContext.instance(shared_secret=shared_secret)
        self.sock = context.socket(zmq.REQ, allow_insecure=allow_insecure)
        self.sock.setsockopt(zmq.LINGER, 0)
        server_ip = gethostbyname(server_host)
        self.sock.connect('tcp://{}:{}'.format(server_ip, server_port))
        self.mainloop_thread = threading.Thread(target=self.mainloop)
        self.mainloop_thread.daemon = True
        self.mainloop_thread.start()
        if isinstance(server_ip, bytes):
            server_ip = server_ip.decode()
        if ipaddress.ip_address(server_ip).is_loopback:
            self.timeout = self.LOCALHOST_TIMEOUT
        else:
            self.timeout = self.REMOTE_TIMEOUT

    def mainloop(self):
        try:
            pid = str(os.getpid()).encode('utf8')
            while True:
                time.sleep(self.HEARTBEAT_INTERVAL)
                self.sock.send(pid, zmq.NOBLOCK)
                if not self.sock.poll(self.timeout * 1000):
                    break
                msg = self.sock.recv()
                if not msg == pid:
                    break
            # sys.stderr.write('Heartbeat failure\n')
            with self.lock:
                os.kill(os.getpid(), signal.SIGTERM)
        except zmq.ContextTerminated:
            # Shutting down:
            self.sock.close(linger=0)
            return


class EventBroker(object):
    """A broker to collect Event.post() messages from anywhere in the process tree
    and broadcast them to subscribers calling event.wait(). There is only one of
    these, at the top level process in the ProcessTree."""

    # A message subscribers can use to confirm their connection
    # (and any subscriptions) have been processed.
    WELCOME_MESSAGE = b'_zprocess_broker_hello\0'

    def __init__(self, bind_address='tcp://0.0.0.0', shared_secret=None,
                 allow_insecure=False):
        context = SecureContext.instance(shared_secret=shared_secret)
        self.frontend = context.socket(zmq.PULL, allow_insecure=allow_insecure)
        self.backend = context.socket(zmq.XPUB, allow_insecure=allow_insecure)

        self.poller = zmq.Poller()
        self.poller.register(self.frontend, zmq.POLLIN)
        self.poller.register(self.backend, zmq.POLLIN)

        self.in_port = self.frontend.bind_to_random_port(bind_address)
        self.out_port = self.backend.bind_to_random_port(bind_address)

        self.mainloop_thread = threading.Thread(target=self.mainloop)
        self.mainloop_thread.daemon = True
        self.mainloop_thread.start()

    def mainloop(self):
        while True:
            try:
                events = dict(self.poller.poll())
                if self.backend in events:
                    msg = self.backend.recv()
                    is_subscription, topic = ord(msg[0:1]), msg[1:]
                    if is_subscription and topic.startswith(self.WELCOME_MESSAGE):
                        # A new subscriber asking for a welcome message to confirm
                        # that we have received all subscriptions made prior to
                        # this request. Send the topic back (it includes a unique
                        # random number to ensure only the recipient gets it)
                        self.backend.send(topic)
                if self.frontend in events:
                    # Forward messages to subscribers:
                    self.backend.send_multipart(self.frontend.recv_multipart())
            except zmq.ContextTerminated:
                # Shutting down:
                self.frontend.close(linger=0)
                self.backend.close(linger=0)
                return


class ExternalBroker(object):
    """Container object for the details of an event broker that is external to
    the ProcessTree that this process belongs to.
    """
    def __init__(self, host, in_port, out_port):
        self.host = host
        self.in_port = in_port
        self.out_port = out_port


class Event(object):

    def __init__(self, process_tree, event_name, role='wait', external_broker=None):
        self.event_name = event_name
        # We null terminate the event name otherwise any subscriber subscribing to
        # an event *starting* with our event name will also receive it, which
        # we do not want:
        self._encoded_event_name = self.event_name.encode('utf8') + b'\0'
        self.role = role
        if not role in ['wait', 'post', 'both']:
            raise ValueError("role must be 'wait', 'post', or 'both'")
        self.can_wait = self.role in ['wait', 'both']
        self.can_post = self.role in ['post', 'both']
        context = SecureContext.instance(shared_secret=process_tree.shared_secret)

        if external_broker is not None:
            broker_host = external_broker.host
            broker_in_port = external_broker.in_port
            broker_out_port = external_broker.out_port
        else:
            process_tree.check_broker()
            broker_host = process_tree.broker_host
            broker_in_port = process_tree.broker_in_port
            broker_out_port = process_tree.broker_out_port

        broker_ip = gethostbyname(broker_host)
        if self.can_wait:
            self.sub = context.socket(zmq.SUB,
                                      allow_insecure=process_tree.allow_insecure)
            self.sub.set_hwm(1000)
            self.sub.setsockopt(zmq.SUBSCRIBE, self._encoded_event_name)
            self.sub.connect('tcp://{}:{}'.format(broker_ip, broker_out_port))
            # Request a welcome message from the broker confirming it receives this
            # subscription request. This is important so that by the time this
            # __init__ method returns, the caller can know for sure that if the
            # broker receives a message, it will definitely be forwarded to the
            # subscribers and not discarded. It is important that this come after
            # connect() and after the other setsockopt for our event subscription,
            # otherwise the two subscription requests may be sent in the opposite
            # order, preventing us from relying on the receipt of a welcome message
            # as confirmation that the other subscription was received. We use a
            # unique random number to prevent treating *other* subscribers' welcome
            # messages as our own. This is a lot of hoops to jump through when it
            # would be really nice if zeroMQ could just have a way of saying "block
            # until all subscription messages processed", which is all we're really
            # doing.
            unique_id = os.urandom(32)
            self.sub.setsockopt(zmq.SUBSCRIBE,
                                EventBroker.WELCOME_MESSAGE + unique_id)
            # Wait until we receive the welcome message. Throw out any messages
            # prior to it. Timeout if we get nothing for 5 seconds:
            while True:
                events = self.sub.poll(flags=zmq.POLLIN, timeout=5000)
                if not events:
                    raise TimeoutError("Could not connect to event broker")
                if self.sub.recv() == EventBroker.WELCOME_MESSAGE + unique_id:
                    break
            # Great, we're definitely connected to the broker now, and it has
            # processed our subscription. Remove the welcome event subscription
            # and proceed:
            self.sub.setsockopt(zmq.UNSUBSCRIBE,
                                EventBroker.WELCOME_MESSAGE + unique_id)
            self.sublock = threading.Lock()
        if self.can_post:
            self.push = context.socket(zmq.PUSH,
                                       allow_insecure=process_tree.allow_insecure)
            self.push.connect('tcp://{}:{}'.format(broker_ip, broker_in_port))
            self.pushlock = threading.Lock()

    def post(self, identifier, data=None):
        if not self.can_post:
            msg = ("Instantiate Event with role='post' " +
                   "or 'both' to be able to post events")
            raise ValueError(msg)
        with self.pushlock:
            self.push.send_multipart([self._encoded_event_name,
                                    str(identifier).encode('utf8'),
                                    pickle.dumps(data,
                                        protocol=zprocess.PICKLE_PROTOCOL)])

    def wait(self, identifier, timeout=None):
        identifier = str(identifier)
        if not self.can_wait:
            msg = ("Instantiate Event with role='wait' " +
                   "or 'both' to be able to wait for events")
            raise ValueError(msg)
        # First check through events that are already in the buffer:
        while True:
            with self.sublock:
                events = self.sub.poll(0, flags=zmq.POLLIN)
                if not events:
                    break
                encoded_event_name, event_id, data = self.sub.recv_multipart()
                event_id = event_id.decode('utf8')
                data = pickle.loads(data)
                assert encoded_event_name == self._encoded_event_name
                if event_id == identifier:
                    return data
        # Since we might have to make several recv() calls before we get the
        # right identifier, we must implement our own timeout:
        start_time = monotonic()
        while timeout is None or (monotonic() < start_time + timeout):
            with self.sublock:
                if timeout is not None:
                    # How long left before the elapsed time is greater than
                    # timeout?
                    remaining = (start_time + timeout - monotonic())
                    poll_timeout = max(0, remaining)
                    events = self.sub.poll(1000 * poll_timeout, flags=zmq.POLLIN)
                    if not events:
                        break
                encoded_event_name, event_id, data = self.sub.recv_multipart()
                event_id = event_id.decode('utf8')
                data = pickle.loads(data)
                assert encoded_event_name == self._encoded_event_name
                if event_id == identifier:
                    return data
        raise TimeoutError('No event received: timed out')


class WriteQueue(object):

    """Provides writing of python objects to the underlying zmq socket, with added
    locking and interruptibility of blocking sends. No reading is supported, once you
    put an object, you can't check what was put or whether the items have been gotten"""

    def __init__(self, sock):
        self.sock = sock
        self.lock = threading.Lock()
        self.poller = zmq.Poller()
        self.poller.register(self.sock)
        self.interruptor = Interruptor()

    def put(self, obj, timeout=None, interruptor=None):
        """Send an object to ourself, with optional timeout and optional
        zprocess.Interruptor instance for interrupting from another thread"""
        if timeout is not None:
            deadline = monotonic() + timeout
        if interruptor is None:
            interruptor = self.interruptor
        with self.lock:
            try:
                interruption_sock = interruptor.subscribe()
                self.poller.register(interruption_sock)
                while True:
                    if timeout is not None:
                        timeout = max(0, (deadline - monotonic()) * 1000)
                    events = dict(self.poller.poll(timeout))
                    if not events:
                        raise TimeoutError('put() timed out')
                    if interruption_sock in events:
                        raise Interrupted(interruption_sock.recv().decode('utf8'))
                    try:
                        return self.sock.send_pyobj(
                            obj, protocol=zprocess.PICKLE_PROTOCOL, flags=zmq.NOBLOCK
                        )
                    except zmq.ZMQError:
                        # Queue became full or we disconnected or something, keep
                        # polling:
                        continue
            finally:
                self.poller.unregister(interruption_sock)
                interruptor.unsubscribe()

    def interrupt(self, reason=None):
        """Interrupt any current and future put() calls, causing them to raise
        Interrupted(reason) until clear_interrupt() is called. Note that if put() was
        called with an externally created Interruptor object, then this method will not
        interrupt that call, and Interruptor.set() will need to be called on the given
        interruptor object instead."""
        self.interruptor.set(reason=reason)

    def clear_interrupt(self):
        """Clear our internal Interruptor object so that future put() calls can proceed
        as normal."""
        self.interruptor.clear()



class ReadQueue(object):
    """Provides reading and writing methods to the underlying zmq socket(s), with added
    locking and interruptibility of blocking recv()s. Items put() to the queue are
    readable by get(), as are items sent from the connected peer. This can be useful to
    send commands to a loop from within the same application. The original use case was
    for breaking out of blocking recv()s, though that use case is now better served by
    interrupt()"""

    def __init__(self, sock, to_self_sock):
        self.sock = sock
        self.to_self_sock = to_self_sock
        self.lock = threading.Lock()
        self.to_self_lock = threading.Lock()
        self.in_poller = zmq.Poller()
        self.in_poller.register(self.sock)
        self.out_poller = zmq.Poller()
        self.out_poller.register(self.to_self_sock)
        self.interruptor = Interruptor()

    def get(self, timeout=None, interruptor=None):
        """Get an object sent from the child, with optional timeout and optional
        zprocess.Interruptor instance for interrupting from another thread. If
        interruptor is not provided, a default interruptor is used, and so get() can be
        interrupted from another thread with ReadQueue.interruptor.set() (remember to
        call .clear() to reset before calling get() again"""
        if timeout is not None:
            timeout *= 1000 # convert to ms
        if interruptor is None:
            interruptor = self.interruptor
        with self.lock:
            try:
                interruption_sock = interruptor.subscribe()
                self.in_poller.register(interruption_sock)
                events = dict(self.in_poller.poll(timeout))
                if not events:
                    raise TimeoutError('get() timed out')
                if interruption_sock in events:
                    raise Interrupted(interruption_sock.recv().decode('utf8'))
                return self.sock.recv_pyobj()
            finally:
                self.in_poller.unregister(interruption_sock)
                interruptor.unsubscribe()

    def put(self, obj, timeout=None, interruptor=None):
        """Send an object to ourself, with optional timeout and optional
        zprocess.Interruptor instance for interrupting from another thread"""
        if timeout is not None:
            deadline = monotonic() + timeout
        if interruptor is None:
            interruptor = self.interruptor
        with self.to_self_lock:
            try:
                interruption_sock = interruptor.subscribe()
                self.out_poller.register(interruption_sock)
                while True:
                    if timeout is not None:
                        timeout = max(0, (deadline - monotonic()) * 1000)
                    events = dict(self.out_poller.poll(timeout))
                    if not events:
                        raise TimeoutError('put() timed out')
                    if interruption_sock in events:
                        raise Interrupted(interruption_sock.recv().decode('utf8'))
                    try:
                        return self.to_self_sock.send_pyobj(
                            obj, protocol=zprocess.PICKLE_PROTOCOL, flags=zmq.NOBLOCK
                        )
                    except zmq.ZMQError:
                        # Queue became full or we disconnected or something, keep
                        # polling:
                        continue
            finally:
                self.out_poller.unregister(interruption_sock)
                interruptor.unsubscribe()

    def interrupt(self, reason=None):
        """Interrupt any current and future get()/put() calls, causing them to raise
        Interrupted(reason) until clear_interrupt() is called. Note that if put()/get()
        was called with an externally created Interruptor object, then this method will
        not interrupt that call, and Interruptor.set() will need to be called on the
        given interruptor object instead."""
        self.interruptor.set(reason=reason)

    def clear_interrupt(self):
        """Clear our internal Interruptor object so that future put()/get() calls can
        proceed as normal."""
        self.interruptor.clear()


class _StreamProxyBuffer(object):
    def __init__(self, streamproxy):
        self.streamproxy = streamproxy

    def close(self):
        return self.streamproxy.close()

    def fileno(self):
        return self.streamproxy.fileno()

    def isatty(self):
        return self.streamproxy.isatty()

    def flush(self):
        return self.streamproxy.flush()

    def write(self, data):
        """Compatibility for code calling stdout.buffer.write etc"""
        if not isinstance(data, bytes):
            raise TypeError("Can only write bytes to buffer")
        return self.streamproxy.write(data)


class StreamProxy(object):

    # Declare that our write() method accepts a 'charformat' kwarg for specifying
    # formatting
    supports_rich_write = True

    def __init__(self, fd, sock, socklock, streamname):
        self.fd = fd
        self.sock = sock
        self.socklock = socklock
        self.streamname_bytes = streamname.encode('utf8')
        # Hopefully this covers all our bases for ensuring the mainloop gets
        # a chance to run after C output before Python output is produced:
        if os.name == 'posix':
            libpthread = ctypes.CDLL(find_library('pthread'))
            self.sched_yield = libpthread.sched_yield
        else:
            self.sched_yield = lambda: time.sleep(0)
        self.buffer = _StreamProxyBuffer(self)

    def write(self, s, charformat=None):
        if isinstance(s, str):
            if PY2:
                s = s.encode('utf8')
            else:
                # Allow binary data embedded in the unicode string via surrogate escapes
                # to turn back into the bytes it originally represented:
                s = s.encode('utf8', errors='surrogateescape')
        if charformat is None:
            charformat = self.streamname_bytes
        elif isinstance(charformat, str):
            charformat = charformat.encode('utf8')
        # Release the GIL momentarily to increase the odds that previous output
        # from C or a subprocess will have been processed by
        # OutputInterceptor._mainloop(), preserving the order of output. This is no
        # guarantee, but is the best we can do if we want to be able to capture
        # output from C and subprocesses whilst distinguishing between stdout and
        # stderr without doing LD_PRELOAD tricks. Sleeping zero or calling
        # sched_yield releases the GIL momentarily, giving other threads a chance
        # to run. The mainloop is higher priority on Windows, so it will get the
        # GIL until it is done or until the next checkinterval(). It is not higher
        # priority on linux, but this is the best we can do.
        self.sched_yield()
        with self.socklock:
            # We write directly to the zmq socket for Python output to guarantee
            # that sys.stdout and sys.stderr will come out in the correct order
            # when coming from Python code, which we cannot similarly guarantee for
            # C output. The downside of this is that Python stdout and C stdout
            # output may be in the incorrect order, though we do our best to
            # decrease the odds of this with thread yielding. This seemed like
            # the better compromise than sending everything through the pipes and
            # possibly reordering stdout and stderr for ordinary Python output.
            self.sock.send_multipart([charformat, s])
        # os.write(self.fd, s)

    def close(self):
        os.close(self.fd)

    def fileno(self):
        return self.fd

    def isatty(self):
        return os.isatty(self.fd)

    def flush(self):
        pass


class OutputInterceptor(object):
    # Two OutputInterceptors talking to the same server must share a sock and
    # corresponding lock:
    socks_by_connection = weakref.WeakValueDictionary()
    locks_by_connection = weakref.WeakValueDictionary()
    # A lock to serialise calls to connect() and disconnect()
    connect_disconnect_lock = threading.Lock()
    # Only one stdout or stderr can be conencted at a time,
    # so we keep track with this class attribute dict:
    streams_connected = {'stdout': None, 'stderr': None}

    # Ensure output is flushed at interpreter shutdown:
    def _close_socks():
        cls = OutputInterceptor
        for connection in list(cls.socks_by_connection.keys()):
            try:
                sock = cls.socks_by_connection[connection]
                lock = cls.locks_by_connection[connection]
            except KeyError:
                continue
            with lock:
                sock.close()

    atexit.register(_close_socks)

    """Redirect stderr and stdout to a zmq PUSH socket"""
    def __init__(self, host, port, streamname='stdout',
                 shared_secret=None, allow_insecure=False):
        if streamname not in ['stdout', 'stderr']:
            msg = "streamname must be 'stdout' or 'stderr'"
            raise ValueError(msg)

        self.streamname = streamname
        self.stream_fd = None
        self.backup_fd = None
        self.read_pipe_fd = None
        self.mainloop_thread = None
        self.shutting_down = False

        ip = gethostbyname(host)
        connection_details = (ip, port, shared_secret, allow_insecure)
        if connection_details not in self.socks_by_connection:
            context = SecureContext.instance(shared_secret=shared_secret)
            sock = context.socket(zmq.PUSH, allow_insecure=allow_insecure)
            # At socket close, allow up to 1 second to send all unsent messages:
            sock.setsockopt(zmq.LINGER, 1000)
            sock.connect('tcp://%s:%d' % (ip, port))
            socklock = threading.Lock()
            self.socks_by_connection[connection_details] = sock
            self.locks_by_connection[connection_details] = socklock
        self.sock = self.socks_by_connection[connection_details]
        self.socklock = self.locks_by_connection[connection_details]

        if os.name == 'nt':
            self._libc = ctypes.cdll.msvcrt
        else:
            self._libc =  ctypes.CDLL(None)

        self._c_stream_ptr = self._get_c_stream()

    def _get_c_stream(self):
        """Get file pointer for C stream"""

        if os.name == 'nt':
            # Windows:
            class FILE(ctypes.Structure):
                _fields_ = [
                    ("_ptr", ctypes.c_char_p),
                    ("_cnt", ctypes.c_int),
                    ("_base", ctypes.c_char_p),
                    ("_flag", ctypes.c_int),
                    ("_file", ctypes.c_int),
                    ("_charbuf", ctypes.c_int),
                    ("_bufsize", ctypes.c_int),
                    ("_tmpfname", ctypes.c_char_p),
                ]
        
            iob_func = getattr(self._libc, '__iob_func')
            iob_func.restype = ctypes.POINTER(FILE)
            iob_func.argtypes = []
            
            array = iob_func()
            if self.streamname == 'stdout':
                return ctypes.addressof(array[1])
            else:
                return ctypes.addressof(array[2])
        else:
            try:
                # Linux:
                return ctypes.c_void_p.in_dll(self._libc, self.streamname)
            except ValueError:
                # MacOS:
                return ctypes.c_void_p.in_dll(self._libc, '__%sp' % self.streamname)

    def _flush(self):
        """Flush the C level file pointer for the stream. This should be
        done before closing their file descriptors"""
        if os.name == 'nt':
            # In windows we flush all output streams by calling flush on a null
            # pointer:
            file_ptr = ctypes.c_void_p()
        else:
            file_ptr = self._c_stream_ptr
        self._libc.fflush(file_ptr)

    def _unbuffer(self):
        """Set C output streams to unbuffered"""
        if os.name == 'nt':
            _IONBF = 4
        else:
            _IONBF = 2
        self._libc.setvbuf.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_size_t,
        ]
        self._libc.setvbuf(self._c_stream_ptr, None, _IONBF, 0)

    def connect(self):
        """Begin output redirection"""
        with self.connect_disconnect_lock:
            if self.streams_connected[self.streamname] is not None:
                msg = ("An OutputInterceptor is already connected for stream " +
                       "'%s'" % self.streamname)
                raise RuntimeError(msg)
            self.streams_connected[self.streamname] = self
            stream = getattr(sys, self.streamname)
            if stream is not None and stream.fileno() > 0:
                self.stream_fd = stream.fileno()
            else:
                # On Windows with pythonw, sys.stdout and sys.stderr are None or
                # have invalid (negative) file descriptors. We still want to
                # redirect any C code or subprocesses writing to stdout or stderr
                # so we use the standard file descriptor numbers, which, assuming
                # no other tricks played by other code, will be 1 and 2:
                if self.streamname == 'stdout':
                    self.stream_fd = 1
                elif self.streamname == 'stderr':
                    self.stream_fd = 2
                else:
                    raise ValueError(self.streamname)
                
            # os.dup() lets us take a sort of backup of the current file
            # descriptor for the stream, so that we can restore it later:
            self.backup_fd = os.dup(self.stream_fd)
                
            # We set up a pipe and set the write end of it to be the output file
            # descriptor. C code and subprocesses will see this as the stream and
            # write to it, and we will read from the read end of the pipe in a 
            # thread to pass their output to the zmq socket.
            self.read_pipe_fd, write_pipe_fd = os.pipe()
            self.mainloop_thread = threading.Thread(target=self._mainloop)
            self.mainloop_thread.daemon = True
            self.mainloop_thread.start()

            # Before doing the redirection, flush the current streams:
            self._flush()
            self._unbuffer()
            # Redirect the stream to our write pipe, closing the original stream
            # file descriptor:
            os.dup2(write_pipe_fd, self.stream_fd)
            # Replace sys.<streamname> with a proxy object. Any Python code writing
            # to the stream will have the output passed directly to the zmq socket,
            # and any other code inspecting sys.stdout/sderr's fileno() will see
            # the write end of our redirection pipe.
            proxy = StreamProxy(write_pipe_fd,
                                self.sock, self.socklock, self.streamname)
            setattr(sys, self.streamname, proxy)

    def disconnect(self):
        """Stop output redirection"""
        with self.connect_disconnect_lock:
            if self.streams_connected[self.streamname] is not self:
                msg = ("This OutputInterceptor not connected for stream " +
                       "'%s'" % self.streamname)
                raise RuntimeError(msg)
            self.streams_connected[self.streamname] = None
            orig_stream = getattr(sys, '__%s__' % self.streamname)
            self._flush()
            os.dup2(self.backup_fd, self.stream_fd)
            self.stream_fd = None
            os.close(self.backup_fd)
            self.backup_fd = None
            proxy_stream = getattr(sys, self.streamname)
            # self-pipe trick to break out of the blocking read and see the
            # shutting_down flag. We do this rather than close the file descriptor
            # for the write end of the pipe because other processes may still have
            # an open file descriptor for the write end of the pipe, so the
            # mainloop would not get a signal that the pipe was closed. Any
            # remaining subprocesses will get a broken pipe error if they try to
            # write output.
            self.shutting_down = True
            os.write(proxy_stream.fileno(), b'<dummy message>')
            proxy_stream.close()
            setattr(sys, self.streamname, orig_stream)
            self.mainloop_thread.join()
            self.mainloop_thread = None
            self.shutting_down = False          

    def _mainloop(self):
        streamname_bytes = self.streamname.encode('utf8')
        # Set the priority of this thread a bit higher so that when C code or
        # subprocesses write to stdout, and multiple threads are waiting on the
        # GIL, this thread will get it and process the output before Python
        # output is produced from other threads. This is only reliable if we
        # can guarantee that the main thread releases and re-acquires the GIL
        # before producing output, which we do in StreamProxy.write().
        if os.name == 'nt':
            w32 = ctypes.windll.kernel32
            THREAD_SET_INFORMATION = 0x20
            THREAD_PRIORITY_ABOVE_NORMAL = 1
            handle = w32.OpenThread(THREAD_SET_INFORMATION, False,
                                    threading.current_thread().ident)
            result = w32.SetThreadPriority(handle, THREAD_PRIORITY_ABOVE_NORMAL)
            w32.CloseHandle(handle)
            if not result:
                print('Failed to set priority of thread',  w32.GetLastError())
        else:
            # In linux/mac, we cannot set a higher priority without root
            # privileges. Best we can do it to call sched_yield from the other
            # thread.
            pass
        while True:
            s = os.read(self.read_pipe_fd, 4096)
            with self.socklock:
                if not s or self.shutting_down:
                    os.close(self.read_pipe_fd)
                    self.read_pipe_fd = None
                    break
                self.sock.send_multipart([streamname_bytes, s])


class RichStreamHandler(logging.StreamHandler):
    """Logging hander that forwards the log level name as the 'charformat' keyword
    argument, if it exists, to the write() method of the underlying stream object. If
    connected to a qtutils.OutputBox, The OutputBox will format different log levels
    depending on severity. This is designed both to work with an OutputBox as the
    stream, or with a zprocess.StreamProxy. Thus zprocess subprocesses using a logger
    with a RichStreamHandler set to sys.stdout or sys.stderr will have colourised log
    output, as will any loggers in the same process as the OutputBox with the stream set
    to the OutputBox."""

    def emit(self, record):
        if not getattr(self.stream, 'supports_rich_write', False):
            return logging.StreamHandler.emit(self, record)
        try:
            msg = self.format(record) + '\n'
            self.stream.write(msg, charformat=record.levelname)
        except Exception:
            self.handleError(record)

            
def rich_print(*values, **kwargs):
    """A print function allowing bold, italics, and colour, if stdout.write or
    stderr.write supports a 'charformat' keyword argument and is connected to a
    qtutils.OutputBox. This method accepts the same arguments as the Python print
    function, as well as keyword args: 'color', a string containing either a named color
    or hex value of a color; 'bold' and 'italic': booleans as to whether the text should
    be bold or italic. If file=sys.stderr, the output will be red. Otherwise, if color
    is not specified, output will be white. The 'color' and 'bold' keyword arguments if
    provided will override the settings inferred from the file keyword argument. If the
    stream does not support the 'charformat' keyword argument, then formatting will be
    ignored."""

    file = kwargs.pop('file', sys.stdout)

    if not getattr(file, 'supports_rich_write', False):
        return print(*values, file=file, **kwargs)

    sep = kwargs.pop('sep', ' ')
    end = kwargs.pop('end', '\n')

    if file is sys.stderr:
        color = 'red'
        bold = False
    else:
        color = 'white'
        bold = False
    bold = kwargs.pop('bold', bold)
    color = kwargs.pop('color', color)
    italic = kwargs.pop('italic', False)
    charformat = repr((color, bold, italic)).encode('utf8')
    file.write(sep.join(str(s) for s in values) + end, charformat=charformat)


class Process(object):
    """A class providing similar functionality to multiprocessing.Process, but
    using zmq for communication and creating processes in a fresh environment
    rather than by forking (or imitation forking as in Windows). Do not override
    its methods other than run()."""

    def __init__(
        self,
        process_tree,
        output_redirection_host=None,
        output_redirection_port=None,
        remote_process_client=None,
        subclass_fullname=None,
        startup_timeout=5,
    ):
        self._redirection_port = output_redirection_port
        self._redirection_host = output_redirection_host
        self.process_tree = process_tree
        self.to_child = None
        self.from_child = None
        self.child = None
        self.parent_host = None
        self.to_parent = None
        self.from_parent = None
        self.kill_lock = None
        self.remote_process_client = remote_process_client
        self.subclass_fullname = subclass_fullname
        self.startup_timeout = startup_timeout
        self.startup_event = None

        self.startup_interruptor = Interruptor()
        self.startup_lock = threading.Lock()
        if subclass_fullname is not None:
            if self.__class__ is not Process:
                msg = (
                    "Can only pass subclass_fullname to Process directly, "
                    + "not to a subclass"
                )
                raise ValueError(msg)
        if (
            self.__module__ == '__main__'
            and self.subclass_fullname is None
            and self.remote_process_client is not None
        ):
            msg = (
                "Cannot start a remote process for a class defined in __main__. "
                + "The remote process will not be able to import the required class "
                + "as it will not know the import path. Either define the class in a "
                + "different module importable on both systems, or use "
                + "zprocess.Process directly, passing in subclass_fullname to specify "
                + "the full import path."
            )
            raise RuntimeError(msg)

    def start(self, *args, **kwargs):
        """Call in the parent process to start a subprocess. Passes args and
        kwargs to the run() method"""
        # Allow Process.terminate() to be called at any time, either before or after
        # start(), and catch it in a race-free way. Process.terminate() will acquire the
        # startup lock and check for the existence of startup_event, and will only block
        # on it if it exists. Otherwise it will call self.interrupt_startup(), and the
        # below code will know not to start the child process.
        with self.startup_lock:
            if self.startup_interruptor.is_set:
                raise Interrupted(self.startup_interruptor.reason)
            else:
                self.startup_event = threading.Event()

        startup_queue = Queue()
        try:
            child_details = self.process_tree.subprocess(
                PROCESS_CLASS_WRAPPER,
                output_redirection_port=self._redirection_port,
                output_redirection_host=self._redirection_host,
                remote_process_client=self.remote_process_client,
                startup_timeout=self.startup_timeout,
                pymodule=True,
                # This argument is not used by the child, but it makes it visible in
                # process lists which process is which:
                args=[self.subclass_fullname or self.__class__.__name__],
                startup_queue=startup_queue,
                startup_interruptor=self.startup_interruptor
            )
            self.to_child, self.from_child, self.child = child_details
        finally:
            with self.startup_lock:
                # If the above was interrupted, but the child object existed before
                # interruption, it will have been put to this queue. If so, set it as
                # self.child
                try:
                    self.child = startup_queue.get(block=False)
                except Empty:
                    pass
                # Inform waiting code that self.child should exist now, and that if it
                # is None it means startup failed before it was created:
                self.startup_event.set()

        # Get the file that the class definition is in (not this file you're
        # reading now, rather that of the subclass):
        if self.subclass_fullname is None:
            module_file = os.path.abspath(sys.modules[self.__module__].__file__)
            basepath, extension = os.path.splitext(module_file)
            if extension == '.pyc':
                module_file = basepath + '.py'
            if not os.path.exists(module_file):
                # Nope? How about this extension then?
                module_file = basepath + '.pyw'
            if not os.path.exists(module_file):
                # Still no? Well we can't really work out what the extension is then,
                # can we?
                msg = ("Can't find module file, what's going on, does " +
                       "it have an unusual extension?")
                raise NotImplementedError(msg)
        else:
            module_file = None # Will be found with import machinery in the subprocess

        if module_file is not None and self.remote_process_client is None:
            # Send the module filepath to the child process so it can execute it in
            # __main__, otherwise class definitions from the users __main__ module will
            # not be unpickleable. Note that though executed in __main__, the code's
            # __name__ will not be __main__, and so any main block won't execute, which
            # is good! Also send sys.path the ensure the child's environment is the same
            # as ours. Do not do this if remote, since the environment will not be
            # meaningful on the other host.
            self.to_child.put(
                [self.__module__, module_file, sys.path],
                timeout=self.startup_timeout,
                interruptor=self.startup_interruptor,
            )
        else:
            self.to_child.put(
                [None, None, None],
                timeout=self.startup_timeout,
                interruptor=self.startup_interruptor,
            )

        # Send the class to the child, either as a pickled class or as the specified
        # full path:
        if self.subclass_fullname is not None:
            self.to_child.put(
                self.subclass_fullname,
                timeout=self.startup_timeout,
                interruptor=self.startup_interruptor,
            )
        else:
            self.to_child.put(
                self.__class__,
                timeout=self.startup_timeout,
                interruptor=self.startup_interruptor,
            )

        response = self.from_child.get(
            timeout=self.startup_timeout, interruptor=self.startup_interruptor
        )
        if response != 'ok':
            msg = "Error in child process importing specified Process subclass:\n\n%s"
            raise Exception(msg % str(response))
            
        self.to_child.put(
            [args, kwargs],
            timeout=self.startup_timeout,
            interruptor=self.startup_interruptor,
        )
        return self.to_child, self.from_child

    def _run(self):
        """Called in the child process to set up the connection with the
        parent"""
        self.to_parent = self.process_tree.to_parent
        self.from_parent = self.process_tree.from_parent
        self.parent_host = self.process_tree.parent_host
        self.kill_lock = self.process_tree.kill_lock
        args, kwargs = self.from_parent.get()
        self.run(*args, **kwargs)

    def interrupt_startup(self, reason='Process.interrupt_startup() called'):
        """Called from the parent process. Interrupt all blocking operations on starting
        the child process, causing Process.start() to raise Interrupted(reason). After
        interruption, self.child may be None if startup was interrupted before the child
        was started, otherwise self.child will be the child Popen object, which could be
        at any stage of setting up its connection with the parent. This method may be
        called multiple times without raising an exception, it will simply do nothing
        if startup has previously been interrupted"""
        with self.startup_lock:
            if not self.startup_interruptor.is_set:
                self.startup_interruptor.set(reason=reason)
        if self.startup_event is not None:
            # start() has been called. Block until self.child exists:
            self.startup_event.wait()

    def terminate(self, wait_timeout=None, **kwargs):
        """Interrupt process startup if not already done, ensuring self.child exists or
        is None if startup was interrupted before the process was created. Then if the
        child is not None, call Popen.terminate() and Popen.wait() on it."""
        self.interrupt_startup(reason='Process.terminate() called')
        if self.child is not None:
            try:
                self.child.terminate(**kwargs)
                self.child.wait(timeout=wait_timeout, **kwargs)
            except (OSError, TimeoutError):
                # process is already dead, or cannot contact remote server
                pass

    def run(self, *args, **kwargs):
        """The method that gets called in the subprocess. To be overridden by
        subclasses"""
        print("This is the run() method of the default zprocess Process class.")
        print("Subclass Process and override this method with your own code "
              "to be run in a subprocess")
        import time
        time.sleep(1)


class ProcessTree(object):
    # __instance will be set to an an instance of ProcessTree after a subprocess sets up
    # its connection with the parent, configured with the details inherited from the
    # parent process. It will not be set in the top-level process. It can be accessed
    # with ProcessTree.instance() and is name-mangled to be class-private.
    __instance = None
    def __init__(
        self,
        shared_secret=None,
        allow_insecure=False,
        zlock_host=None,
        zlock_port=ZLOCK_DEFAULT_PORT,
        zlog_host=None,
        zlog_port=ZLOG_DEFAULT_PORT,
    ):
        self.shared_secret = shared_secret
        self.allow_insecure = allow_insecure
        self.zlock_host = zlock_host
        self.zlock_port = zlock_port
        self.zlog_host = zlog_host
        self.zlog_port = zlog_port
        self.parent_host = None
        self.broker = None
        self.broker_host = None
        self.broker_in_port = None
        self.broker_out_port = None
        self.heartbeat_server = None
        self.heartbeat_client = None
        self.output_redirection_host = None
        self.output_redirection_port = None
        self.remote_output_receiver = None
        self.to_parent = None
        self.from_parent = None
        self.kill_lock = None
        self.log_paths = {}
        self.startup_timeout = None

        self.zlock_client = None
        if self.zlock_host is not None:
            self.zlock_client = ZLockClient(
                self.zlock_host,
                self.zlock_port,
                shared_secret=self.shared_secret,
                allow_insecure=self.allow_insecure,
            )

        self.zlog_client = None
        if self.zlog_host is not None:
            self.zlog_client = ZLogClient(
                self.zlog_host,
                self.zlog_port,
                shared_secret=self.shared_secret,
                allow_insecure=self.allow_insecure,
            )

    @classmethod
    def instance(cls):
        return ProcessTree.__instance

    def check_broker(self):
        if self.broker_in_port is None:
            # We don't have a parent with a broker: it is our responsibility to
            # make a broker:
            self.broker = EventBroker(shared_secret=self.shared_secret,
                                      allow_insecure=self.allow_insecure)
            self.broker_host = 'localhost'
            self.broker_in_port = self.broker.in_port
            self.broker_out_port = self.broker.out_port

    def event(self, event_name, role='wait', external_broker=None):
        return Event(self, event_name, role=role, external_broker=external_broker)

    def remote_process_client(self, host, port=REMOTE_DEFAULT_PORT):
        """Return a RemoteProcessClient configured with this ProcessTree's security
        settings"""
        return RemoteProcessClient(
            host,
            port=port,
            shared_secret=self.shared_secret,
            allow_insecure=self.allow_insecure,
        )

    def lock(self, key, read_only=False):
        """Return a zprocess.locking.Lock for a resource identified by the given key.
        This lock is exclusive among all processes configured to use the same key and
        same zlock server as this ProcessTree."""
        if self.zlock_client is None:
            msg = "ProcessTree not configured to connect to a zlock server"
            raise RuntimeError(msg)
        return self.zlock_client.lock(key, read_only=read_only)

    def logging_handler(self, filepath, name=None):
        """Return a zprocess.zlog.ZMQLoggingHandler for the given filepath. All loggers
        using the same file and zlog server as this ProcessTree will log to the same
        file in a race-free way. If name is not None and matches the name passed in when
        creating a logging handler in a parent process, the filepath that was used in
        the parent process will be used instead of the one passed to this method. In
        this way, if parent and child processes are running on different computers, the
        filepath passed in by the parent will be used. This way one can prevent the zlog
        server creating additional log files on the computer that the toplevel process
        is running on, with paths that may be unrelated to other paths on that
        computer."""
        if name is not None:
            filepath = self.log_paths.setdefault(name, filepath)
        if self.zlog_host is None:
            msg = "ProcessTree not configured to connect to a zlog server"
            raise RuntimeError(msg)
        return self.zlog_client.handler(filepath)

    def subprocess(
        self,
        path,
        output_redirection_host=None,
        output_redirection_port=None,
        remote_process_client=None,
        startup_timeout=5,
        pymodule=False,
        args=None,
        startup_queue=None,
        startup_interruptor=None,
    ):
        """Start a subprocess and set up communication with it. Path can be either a
        path to a Python script to be executed with 'python some_path.py, or a fully
        qualified module path to be executed as 'python -m some.path'. Path will be
        interpreted as the latter only if pymodule=True. If output_redirection_port is
        not None, then all stdout and stderr of the child process will be sent on a
        zmq.PUSH socket to the given port. If startup_queue is provided, it should be a
        queue.Queue(). Once the child process exists, the child Popen object will be
        put() to this queue. This allows Process.interrupt_startup() to obtain and set
        Process.child to the child Popen object, if it exists at the time of
        interruption, even if this function does not return due to raising the
        Intertupted exception. This can be important as code calling
        Process.interrupt_startup() (such as Process.terminate()) may wish to terminate
        the child process. TODO finish this and other docstrings."""
        context = SecureContext.instance(shared_secret=self.shared_secret)
        to_child = context.socket(zmq.PUSH, allow_insecure=self.allow_insecure)
        from_child = context.socket(zmq.PULL, allow_insecure=self.allow_insecure)
        to_self = context.socket(zmq.PUSH, allow_insecure=self.allow_insecure)

        from_child_port = from_child.bind_to_random_port('tcp://0.0.0.0')
        to_self.connect('tcp://127.0.0.1:%s' % from_child_port)
        to_child_port = to_child.bind_to_random_port('tcp://0.0.0.0')
        self.check_broker()
        if self.heartbeat_server is None:
            # First child process, we need a heartbeat server:
            self.heartbeat_server = HeartbeatServer(shared_secret=self.shared_secret)

        if self.zlock_client is not None:
            zlock_process_name = self.zlock_client.process_name
        else:
            zlock_process_name = ''

        if output_redirection_host is None:
            if output_redirection_port is not None:
                # Port provided but no host. Assume this host.
                output_redirection_host = 'localhost'
            elif self.output_redirection_port is not None:
                # No redirection specified. Use existing redirection.
                output_redirection_host = self.output_redirection_host
                output_redirection_port = self.output_redirection_port
            elif remote_process_client is not None:
                # No existing redirection, and process is remote. Redirect its output to
                # our own stdout using a RemoteOutputReceiver.
                if self.remote_output_receiver is None:
                    self.remote_output_receiver = RemoteOutputReceiver(
                        shared_secret=self.shared_secret,
                        allow_insecure=self.allow_insecure,
                    )
                output_redirection_host = 'localhost'
                output_redirection_port = self.remote_output_receiver.port
            else:
                # No existing redirection, process is local. Do not do any redirection.
                output_redirection_host = None
                output_redirection_port = None

        # Note. Any entries in the below dict containing a hostname or IP address must
        # have a key ending in '_host', in order for the below code to translate them
        # into externally valid IP addresses.
        parentinfo = {
            'parent_host': 'localhost',
            'to_parent_port': from_child_port,
            'from_parent_port': to_child_port,
            'heartbeat_server_host': 'localhost',
            'heartbeat_server_port': self.heartbeat_server.port,
            'broker_host': self.broker_host,
            'broker_in_port': self.broker_in_port,
            'broker_out_port': self.broker_out_port,
            'output_redirection_host': output_redirection_host,
            'output_redirection_port': output_redirection_port,
            'shared_secret': self.shared_secret,
            'allow_insecure': self.allow_insecure,
            'zlock_host': self.zlock_host,
            'zlock_port': self.zlock_port,
            'zlock_process_name': zlock_process_name,
            'zlog_host': self.zlog_host,
            'zlog_port': self.zlog_port,
            'log_paths': self.log_paths,
            'startup_timeout': startup_timeout,
        }

        if remote_process_client is not None:
            # Translate any internal hostnames or IP addresses in parentinfo into our
            # external IP address as seen from the remote process server:
            external_ip = remote_process_client.get_external_IP(
                get_interruptor=startup_interruptor
            )
            for key, value in list(parentinfo.items()):
                if key.endswith('_host') and value is not None:
                    ip = gethostbyname(value)
                    if isinstance(ip, bytes):
                        ip = ip.decode()
                    if ipaddress.ip_address(ip).is_loopback:
                        parentinfo[key] = external_ip

        if args is None:
            args = []

        # Build command line args:
        if pymodule:
            cmd = ['-m', path] + args
        else:
            cmd = [os.path.abspath(path)] + args

        # Add environment variable for parent connection details:
        extra_env = {'ZPROCESS_PARENTINFO': json.dumps(parentinfo)}
        if PY2:
            # Windows Python 2, only bytestrings allowed:
            extra_env = {k.encode(): v.encode() for k, v in extra_env.items()}

        if remote_process_client is None:
            env = os.environ.copy()
            env.update(extra_env)
            child = subprocess.Popen([sys.executable] + cmd, env=env)
        else:
            # The remote server will prefix the path to its own Python interpreter.
            # Also, it will pass to Popen an env consisting of its own env updated with
            # this extra_env dict we are passing in.
            child = remote_process_client.Popen(
                cmd,
                prepend_sys_executable=True,
                extra_env=extra_env,
                get_interruptor=startup_interruptor,
            )

        to_child = WriteQueue(to_child)
        from_child = ReadQueue(from_child, to_self)
        if startup_queue is not None:
            startup_queue.put(child)
        try:
            msg = from_child.get(startup_timeout, interruptor=startup_interruptor)
        except TimeoutError:
            raise RuntimeError('child process did not connect within the timeout.')
        assert msg == 'hello'
        return to_child, from_child, child

    def _connect_to_parent(self, parentinfo):

        self.parent_host = gethostbyname(parentinfo['parent_host'])

        if self.zlock_client is not None:
            name = parentinfo['zlock_process_name']
            # Append '-sub' to indicate we're a subprocess of the other process.
            if not name.endswith('-sub'):
                name += '-sub'
            self.zlock_client.set_process_name(name)

        context = SecureContext.instance(shared_secret=self.shared_secret)
        to_parent = context.socket(zmq.PUSH, allow_insecure=self.allow_insecure)
        from_parent = context.socket(zmq.PULL, allow_insecure=self.allow_insecure)
        to_self = context.socket(zmq.PUSH, allow_insecure=self.allow_insecure)

        port_to_self = to_self.bind_to_random_port('tcp://127.0.0.1')
        from_parent.connect(
            'tcp://%s:%d' % (self.parent_host, parentinfo['from_parent_port'])
        )
        from_parent.connect('tcp://127.0.0.1:%d' % port_to_self)
        to_parent.connect(
            "tcp://%s:%s" % (self.parent_host, parentinfo['to_parent_port'])
        )

        self.startup_timeout = parentinfo.get('startup_timeout', None)
        self.from_parent = ReadQueue(from_parent, to_self)
        self.to_parent = WriteQueue(to_parent)
        self.to_parent.put('hello', timeout=self.startup_timeout)

        self.output_redirection_host = parentinfo.get('output_redirection_host', None)
        self.output_redirection_port = parentinfo.get('output_redirection_port', None)
        if self.output_redirection_port is not None:
            stdout = OutputInterceptor(self.output_redirection_host,
                                       self.output_redirection_port,
                                       shared_secret=self.shared_secret,
                                       allow_insecure=self.allow_insecure)
            stderr = OutputInterceptor(self.output_redirection_host,
                                       self.output_redirection_port,
                                       streamname='stderr',
                                       shared_secret=self.shared_secret,
                                       allow_insecure=self.allow_insecure)
            stdout.connect()
            stderr.connect()

        heartbeat_server_host = parentinfo['heartbeat_server_host']
        heartbeat_server_port = parentinfo['heartbeat_server_port']
        self.heartbeat_client = HeartbeatClient(
            heartbeat_server_host,
            heartbeat_server_port,
            shared_secret=self.shared_secret,
            allow_insecure=self.allow_insecure,
        )

        self.broker_host = parentinfo['broker_host']
        self.broker_in_port = parentinfo['broker_in_port']
        self.broker_out_port = parentinfo['broker_out_port']
        self.kill_lock = self.heartbeat_client.lock
        self.log_paths = parentinfo['log_paths']

    @classmethod
    def connect_to_parent(cls):
        if ProcessTree.__instance is not None:
            msg = "Cannot connect_to_parent() twice"
            raise ValueError(msg)

        try:
            parentinfo = json.loads(os.environ['ZPROCESS_PARENTINFO'])
            # Ensure environment variable not inherited by child processes:
            del os.environ['ZPROCESS_PARENTINFO']
        except KeyError:
            msg = "No ZPROCESS_PARENTINFO environment variable found"
            raise RuntimeError(msg)

        process_tree = cls(
            shared_secret=parentinfo['shared_secret'],
            allow_insecure=parentinfo['allow_insecure'],
            zlock_host=parentinfo['zlock_host'],
            zlock_port=parentinfo['zlock_port'],
            zlog_host=parentinfo['zlog_host'],
            zlog_port=parentinfo['zlog_port'],
        )

        process_tree._connect_to_parent(parentinfo)

        ProcessTree.__instance = process_tree
        return process_tree





# Backwards compatability follows:

# A default process tree for all out backward compat functions to work with:
_default_process_tree = ProcessTree(allow_insecure=True)

# Allow instantiating an Event without a ProcessTree as the first argument,
# insert a default ProcessTree:
_Event = Event
class Event(_Event):
    def __init__(self, *args, **kwargs):
        # Convert the keyword argument renaming:
        if 'type' in kwargs:
            kwargs['role'] = kwargs['type']
            del kwargs['type']
        if not args or not isinstance(args[0], ProcessTree):
            args = (_default_process_tree,) + args
        _Event.__init__(self, *args, **kwargs)

# Allow instantiating a Process() without a ProcessTree as the first argument,
# insert a default ProcessTree:
_Process = Process
class Process(_Process):
    def __init__(self, *args, **kwargs):
        # Backward compat for redirection port as sole positional arg:
        if len(args) == 1 and not isinstance(args[0], ProcessTree) and not kwargs:
            kwargs['output_redirection_port'] = args[0]
            args = ()
        if 'process_tree' not in kwargs and (
            not args or not isinstance(args[0], ProcessTree)
        ):
            args = (_default_process_tree,) + args
        _Process.__init__(self, *args, **kwargs)
        
    def _run(self):
        # Set the process tree as the default for this process:
        global _default_process_tree
        _default_process_tree = self.process_tree
        _Process._run(self)

# New way is to call ProcessTree.connect_to_parent(lock) and get back a
# ProcessTree. This is the old way, returning queues and (optionally) a lock
# instead:
def setup_connection_with_parent(lock=False):
    process_tree = ProcessTree.connect_to_parent()
    # Set as the default for this process:
    global _default_process_tree
    _default_process_tree = process_tree
    if lock:
        return (process_tree.to_parent,
                process_tree.from_parent,
                process_tree.kill_lock)
    else:
        return process_tree.to_parent, process_tree.from_parent

# New way is to instantiate a ProcessTree and call
# process_tree.subprocess(). Old way is:
def subprocess_with_queues(path, output_redirection_port=None):
    if output_redirection_port == 0:  # This used to mean no redirection
        output_redirection_port = None
    return _default_process_tree.subprocess(
        path, output_redirection_port=output_redirection_port
    )


__all__ = ['Process', 'ProcessTree', 'setup_connection_with_parent',
           'subprocess_with_queues', 'Event']


# if __name__ == '__main__':
    # def foo():
    #     remote_client = RemoteProcessClient('localhost', allow_insecure=True)
    #     to_child, from_child, child = _default_process_tree.subprocess(
    #         'test_remote.py', remote_process_client=remote_client)
    # foo()

    
