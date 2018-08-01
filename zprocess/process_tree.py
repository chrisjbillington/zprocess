from __future__ import division, unicode_literals, print_function, absolute_import
import sys
PY2 = sys.version_info.major == 2
import os
import threading
import subprocess
import time
import signal
import weakref
import ast
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

import zprocess
from zprocess.clientserver import ZMQClient
from zprocess.security import SecureContext
from zprocess.utils import TimeoutError

PY2 = sys.version_info[0] == 2
if PY2:
    import cPickle as pickle
    str = unicode
else:
    import pickle


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
    """A heartbeating thread that terminates the process if it doesn't get the
    heartbeats back within one second, unless a lock is held."""
    def __init__(self, server_host, server_port, lock=False,
                 shared_secret=None, allow_insecure=False):
        if lock:
            self.lock = threading.Lock()
        else:
            self.lock = None
        context = SecureContext.instance(shared_secret=shared_secret)
        self.sock = context.socket(zmq.REQ, allow_insecure=allow_insecure)
        self.sock.setsockopt(zmq.LINGER, 0)
        server_ip = gethostbyname(server_host)
        self.sock.connect('tcp://{}:{}'.format(server_ip, server_port))
        self.mainloop_thread = threading.Thread(target=self.mainloop)
        self.mainloop_thread.daemon = True
        self.mainloop_thread.start()

    def mainloop(self):
        try:
            pid = str(os.getpid()).encode('utf8')
            while True:
                time.sleep(1)
                self.sock.send(pid, zmq.NOBLOCK)
                if not self.sock.poll(1000):
                    break
                msg = self.sock.recv()
                if not msg == pid:
                    break
            # sys.stderr.write('Heartbeat failure\n')
            if self.lock is not None:
                with self.lock:
                    os.kill(os.getpid(), signal.SIGTERM)
            else:
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
        # and event *starting* with our event name will also receive it, which
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
            # Allow 5 seconds to connect to the Broker:
            events = self.sub.poll(flags=zmq.POLLIN, timeout=5000)
            if not events:
                raise TimeoutError("Could not connect to event broker")
            assert self.sub.recv() == EventBroker.WELCOME_MESSAGE + unique_id
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
        start_time = time.time()
        while timeout is None or (time.time() < start_time + timeout):
            with self.sublock:
                if timeout is not None:
                    # How long left before the elapsed time is greater than
                    # timeout?
                    remaining = (start_time + timeout - time.time())
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

    """Provides writing of python objects to the underlying zmq socket,
    with added locking. No reading is supported, once you put an object,
    you can't check what was put or whether the items have been gotten"""

    def __init__(self, sock):
        self.sock = sock
        self.lock = threading.Lock()

    def put(self, obj):
        with self.lock:
            self.sock.send_pyobj(obj, protocol=zprocess.PICKLE_PROTOCOL)


class ReadQueue(object):
    """Provides reading and writing methods to the underlying zmq socket,
    with added locking. Actually there are two sockets, one for reading,
    one for writing. The only real use case for writing is when the
    read socket is blocking, but the process at the other end has died,
    and you need to stop the thread that is blocking on the read. So
    you send it a quit signal with put()."""

    def __init__(self, sock, to_self_sock):
        self.sock = sock
        self.to_self_sock = to_self_sock
        self.socklock = threading.Lock()
        self.to_self_sock_lock = threading.Lock()

    def get(self, timeout=None):
        with self.socklock:
            if timeout is not None:
                if not self.sock.poll(timeout*1000):
                    raise TimeoutError('get() timed out')
            obj = self.sock.recv_pyobj()
        return obj

    def put(self, obj):
        with self.to_self_sock_lock:
            self.to_self_sock.send_pyobj(obj, protocol=zprocess.PICKLE_PROTOCOL)


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

    def write(self, s, charformat=None):
        if isinstance(s, str):
            s = s.encode('utf8')
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

    def _flush(self):
        """Flush the C level file pointer for the stream. This should be
        done before closing their file descriptors"""
        if os.name == 'nt':
            # Windows:
            libc = ctypes.cdll.msvcrt
            # In windows we flush all output streams by calling flush on a null
            # pointer:
            file_ptr = ctypes.c_void_p()
        else:
            libc = ctypes.CDLL(None)
            try:
                # Linux:
                file_ptr = ctypes.c_void_p.in_dll(libc, self.streamname)
            except ValueError:
                # MacOS:
                file_ptr = ctypes.c_void_p.in_dll(libc, '__%sp' % self.streamname)
        libc.fflush(file_ptr)

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
    with a OutputBoxHandler set to sys.stdout or sys.stderr will have colourised log
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

    def __init__(self, process_tree, output_redirection_port=None,
                 remote_process_client=None):
        self._redirection_port = output_redirection_port
        self.process_tree = process_tree
        self.to_child = None
        self.from_child = None
        self.child = None
        self.to_parent = None
        self.from_parent = None
        self.kill_lock = None
        self.remote_process_client = remote_process_client

    def start(self, *args, **kwargs):
        """Call in the parent process to start a subprocess. Passes args and
        kwargs to the run() method"""
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            'process_class_wrapper.py')
        child_details = self.process_tree.subprocess(path,
                            output_redirection_port=self._redirection_port,
                            remote_process_client=self.remote_process_client)
        self.to_child, self.from_child, self.child = child_details
        # Get the file that the class definition is in (not this file you're
        # reading now, rather that of the subclass):
        module_file = os.path.abspath(sys.modules[self.__module__].__file__)
        basepath, extension = os.path.splitext(module_file)
        if extension == '.pyc':
            module_file = basepath + '.py'
        if not os.path.exists(module_file):
            # Nope? How about this extension then?
            module_file = basepath + '.pyw'
        if not os.path.exists(module_file):
            # Still no? Well I can't really work out what the extension is then,
            # can I?
            msg = ("Can't find module file, what's going on, does " +
                   "it have an unusual extension?")
            raise NotImplementedError(msg)
        # Send it to the child process so it can execute it in __main__,
        # otherwise class definitions from the users __main__ module will not be
        # unpickleable. Note that though executed in __main__, the code's
        # __name__ will not be __main__, and so any main block won't execute,
        # which is good!
        self.to_child.put([self.__module__, module_file, sys.path])
        self.to_child.put(self.__class__)
        self.to_child.put([args, kwargs])
        return self.to_child, self.from_child

    def _run(self):
        """Called in the child process to set up the connection with the
        parent"""
        self.to_parent = self.process_tree.to_parent
        self.from_parent = self.process_tree.from_parent
        self.kill_lock = self.process_tree.kill_lock
        args, kwargs = self.from_parent.get()
        self.run(*args, **kwargs)

    def terminate(self):
        try:
            self.child.terminate()
            self.child.wait()
        except OSError:
            pass  # process is already dead

    def run(self, *args, **kwargs):
        """The method that gets called in the subprocess. To be overridden by
        subclasses"""
        pass


class RemoteChildProxy(object):
    def __init__(self, remote_process_client, pid):
        """Class to wrap operations on a remote subprocess"""
        self.client = remote_process_client
        self.pid = pid

    def terminate(self):
        return self.client.request('terminate', self.pid)

    def kill(self):
        return self.client.request('kill', self.pid)

    def wait(self, timeout=None):
        # The server will only do 0.01 second timeouts at a time to not be blocked
        # from other requests, so we will make requests at 0.1 second intervals to
        # reach whatever the requested timeout was:
        if timeout is not None:
            end_time = time.time() + timeout
        while True:
            try:
                return self.client.request('wait', self.pid)
            except TimeoutError:
                if timeout is not None and time.time() > end_time:
                    raise
                else:
                    time.sleep(0.1)

    def poll(self):
        return self.client.request('poll', self.pid)

    @property
    def returncode(self):
        return self.client.request('returncode', self.pid)

    def __del__(self):
        print('del!')
        self.client.request('__del__', self.pid)


class RemoteProcessClient(zprocess.clientserver.ZMQClient):
    """A class to represent communication with a RemoteProcessServer"""
    def __init__(self, host, port=7340, shared_secret=None, allow_insecure=False):
        zprocess.clientserver.ZMQClient.__init__(self, shared_secret=shared_secret,
                                                 allow_insecure=allow_insecure)
        self.host = host
        self.port = port
        self.shared_secret = shared_secret

    def request(self, command, *args, **kwargs):
        return self.get(self.port, host=self.host,
                        data=[command, args, kwargs],
                        timeout=5)

    def Popen(self, command):
        pid = self.request('Popen', command)
        return RemoteChildProxy(self, pid)


class ProcessTree(object):
    def __init__(self, shared_secret=None, allow_insecure=False):
        self.shared_secret = shared_secret
        self.allow_insecure = allow_insecure
        self.broker = None
        self.broker_host = None
        self.broker_in_port = None
        self.broker_out_port = None
        self.heartbeat_server = None
        self.heartbeat_client = None
        self.to_parent = None
        self.from_parent = None
        self.kill_lock = None

    def _check_broker(self):
        if self.broker_in_port is None:
            # We don't have a parent with a broker: it is our responsibility to
            # make a broker:
            self.broker = EventBroker(shared_secret=self.shared_secret,
                                      allow_insecure=self.allow_insecure)
            self.broker_host = 'localhost'
            self.broker_in_port = self.broker.in_port
            self.broker_out_port = self.broker.out_port

    def event(self, event_name, role='wait', external_broker=None):
        if external_broker is None:
            self._check_broker()
        return Event(self, event_name, role=role, external_broker=external_broker)

    def subprocess(self, path, output_redirection_port=None,
                   remote_process_client=None):
        context = SecureContext.instance(shared_secret=self.shared_secret)
        to_child = context.socket(zmq.PUSH, allow_insecure=self.allow_insecure)
        from_child = context.socket(zmq.PULL, allow_insecure=self.allow_insecure)
        to_self = context.socket(zmq.PUSH, allow_insecure=self.allow_insecure)

        from_child_port = from_child.bind_to_random_port('tcp://127.0.0.1')
        to_self.connect('tcp://127.0.0.1:%s' % from_child_port)
        to_child_port = to_child.bind_to_random_port('tcp://127.0.0.1')
        self._check_broker()
        if self.heartbeat_server is None:
            # First child process, we need a heartbeat server:
            self.heartbeat_server = HeartbeatServer(
                                        shared_secret=self.shared_secret)

        #TODO: fix this:
        # If a custom process identifier has been set in zlock, ensure the child
        # inherits it:
        try:
            zlock = sys.modules['zprocess.zlock']
            zlock_process_identifier_prefix = zlock.process_identifier_prefix
        except KeyError:
            zlock_process_identifier_prefix = ''

        parentinfo = {'parent_host': 'localhost',
                      'to_parent_port': from_child_port,
                      'from_parent_port': to_child_port,
                      'heartbeat_server_host': 'localhost',
                      'heartbeat_server_port': self.heartbeat_server.port,
                      'broker_host': 'localhost',
                      'broker_in_port': self.broker_in_port,
                      'broker_out_port': self.broker_out_port,
                      'output_redirection_host': 'localhost',
                      'output_redirection_port': output_redirection_port,
                      'shared_secret': self.shared_secret,
                      'allow_insecure': self.allow_insecure,
                      'zlock_process_identifier_prefix':
                          zlock_process_identifier_prefix,
                      }

        command = [sys.executable, os.path.abspath(path),
                   '--zprocess-parentinfo', json.dumps(parentinfo)]

        if remote_process_client is None:
            child = subprocess.Popen(command)
        else:
            raise NotImplementedError("This feature is not yet complete")
            child = remote_process_client.Popen(command)

        # The child has 15 seconds to connect to us:
        events = from_child.poll(15000)
        if not events:
            raise RuntimeError('child process did not connect within the timeout.')
        assert from_child.recv() == b'hello'

        to_child = WriteQueue(to_child)
        from_child = ReadQueue(from_child, to_self)

        return to_child, from_child, child

    def _connect_to_parent(self, lock, parentinfo):

        # If a custom process identifier has been set in zlock, ensure we
        # inherit it:
        zlock_pid_prefix = parentinfo.get('zlock_process_identifier_prefix', None)
        if zlock_pid_prefix is not None:
            import zprocess.locking
            # Append '-sub' to indicate we're a subprocess, if it's not already
            # there
            if not zlock_pid_prefix.endswith('sub'):
                zlock_pid_prefix += 'sub'
            # Only set it if the user has not already set it to something in
            # this process:
            if not zprocess.locking.process_identifier_prefix:
                zprocess.locking.set_client_process_name(zlock_pid_prefix)

        context = SecureContext.instance(shared_secret=self.shared_secret)
        to_parent = context.socket(zmq.PUSH, allow_insecure=self.allow_insecure)
        from_parent = context.socket(zmq.PULL, allow_insecure=self.allow_insecure)
        to_self = context.socket(zmq.PUSH, allow_insecure=self.allow_insecure)

        port_to_self = to_self.bind_to_random_port('tcp://127.0.0.1')
        from_parent.connect('tcp://127.0.0.1:%d' % parentinfo['from_parent_port'])
        from_parent.connect('tcp://127.0.0.1:%d' % port_to_self)
        to_parent.connect("tcp://127.0.0.1:%s" %  parentinfo['to_parent_port'])
        to_parent.send(b'hello')

        self.from_parent = ReadQueue(from_parent, to_self)
        self.to_parent = WriteQueue(to_parent)

        output_redirection_host = parentinfo.get('output_redirection_host', None)
        output_redirection_port = parentinfo.get('output_redirection_port', None)
        if output_redirection_port is not None:
            stdout = OutputInterceptor(output_redirection_host,
                                       output_redirection_port,
                                       shared_secret=self.shared_secret,
                                       allow_insecure=self.allow_insecure)
            stderr = OutputInterceptor(output_redirection_host,
                                       output_redirection_port,
                                       streamname='stderr',
                                       shared_secret=self.shared_secret,
                                       allow_insecure=self.allow_insecure)
            stdout.connect()
            stderr.connect()

        heartbeat_server_host = parentinfo['heartbeat_server_host']
        heartbeat_server_port = parentinfo['heartbeat_server_port']
        self.heartbeat_client = HeartbeatClient(heartbeat_server_host,
                                                heartbeat_server_port,
                                                shared_secret=self.shared_secret,
                                                allow_insecure=self.allow_insecure,
                                                lock=lock)

        self.broker_host = parentinfo['broker_host']
        self.broker_in_port = parentinfo['broker_in_port']
        self.broker_out_port = parentinfo['broker_out_port']
        self.kill_lock = self.heartbeat_client.lock

    @classmethod
    def connect_to_parent(cls, lock=False):
        for i, arg in enumerate(sys.argv):
            if arg == '--zprocess-parentinfo':
                parentinfo = json.loads(sys.argv[i+1])
                break
        else:
            msg = "No zprocess parent info in command line args"
            raise RuntimeError(msg)

        process_tree = cls(shared_secret=parentinfo['shared_secret'],
                           allow_insecure=parentinfo['allow_insecure'])

        process_tree._connect_to_parent(lock, parentinfo)

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
        if not args or not isinstance(args[0], ProcessTree):
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
    process_tree = ProcessTree.connect_to_parent(lock)
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
    if output_redirection_port == 0: # This used to mean no redirection
        output_redirection_port = None
    return _default_process_tree.subprocess(path, output_redirection_port)



__all__ = ['Process', 'ProcessTree', 'setup_connection_with_parent',
           'subprocess_with_queues', 'Event']


# if __name__ == '__main__':
    # def foo():
    #     remote_client = RemoteProcessClient('localhost', allow_insecure=True)
    #     to_child, from_child, child = _default_process_tree.subprocess(
    #         'test_remote.py', remote_process_client=remote_client)
    # foo()

    
