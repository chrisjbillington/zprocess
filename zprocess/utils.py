from __future__ import division, unicode_literals, print_function, absolute_import
import sys
import os
import threading
import logging, logging.handlers
from binascii import hexlify
import socket
import tempfile

import zmq

PY2 = sys.version_info[0] == 2
if PY2:
    import subprocess32 as subprocess
    if os.name == 'nt':
        subprocess.DETACHED_PROCESS = 0x00000008
        subprocess.CREATE_NO_WINDOW = 0x08000000
        subprocess.CREATE_NEW_CONSOLE = 0x00000010
    str = unicode
else:
    import subprocess



class TimeoutError(zmq.ZMQError):
    pass


class Interrupted(RuntimeError):
    pass


class Interruptor(object):
    """An object that can be passed to the put() and get() methods of ReadQueue and
    WriteQueue objects, and the get() methods ZMQClient objects, in order to be able to
    interrupt these potentially blocking methods from another thread. Upon calling
    Interruptor.set(), all currently blocking threads calling the aforementioned
    put()/get() methods will raise Interrupted, and all subsequent calls to put()/get()
    with this interruptor will raise Interrupted immediately, until the clear() method
    is called.

    This class may be used to interrupt other blocking zmq operations in user code as
    well, it is not specific to ZMQClient, ReadQueue and WriteQueue. The semantics are
    the following. A thread about to do blocking IO should call Interruptor.subscribe(),
    which will return a socket. The caller can then poll that socket for an interruption
    message. If a message arrives, it will contain the reason, if any, for interruption.
    zprocess classes then raise zprocess.Interrupted(reason), but calling code may do
    with the interruption what they like. The caller must call Interruptor.unsubscribe()
    once blocking zmq operations have completed, regardless of whether an interruption
    was received or not. Calling code should also unregister the interruption socket
    from the zmq.Poller() used to poll. """

    def __init__(self):
        # make a send socket
        self._ctx = zmq.Context.instance()
        self._xpub = self._ctx.socket(zmq.XPUB)
        self._endpoint = 'inproc://zpInterruptor' + hexlify(os.urandom(8)).decode()
        self._xpub.bind(self._endpoint)
        self._local = threading.local()
        self._lock = threading.Lock()
        self.reason = ''
        self.is_set = False

    def subscribe(self):
        """Called by put()/get() methods. Return a thread-local inproc socket subscribed
        to interrupt messages."""
        with self._lock:
            if not hasattr(self._local, 'sub'):
                self._local.sub = self._ctx.socket(zmq.SUB)
                self._local.sub.connect(self._endpoint)
                self._local.subscribed = False
            if self._local.subscribed:
                msg = "Thread already subscribed. Did you forget to call unsubscribe()?"
                raise RuntimeError(msg)
            self._local.sub.subscribe(b'')
            # Ensure subscription is processed:
            self._xpub.recv()
            self._local.subscribed = True
            # If we're already set, send an interruption message immediately:
            if self.is_set:
                self._xpub.send(self.reason.encode('utf8'))
        return self._local.sub

    def unsubscribe(self):
        """Called by put()/get() upon interruption or end of blocking IO to ubsubscribe
        from interrupt messages. This is somewhat important so that messages do not pile
        up"""
        if not hasattr(self._local, 'sub') or not self._local.subscribed:
            raise RuntimeError('not subscribed')
        self._local.subscribed = False
        self._local.sub.unsubscribe(b'')

    def set(self, reason=None):
        """Send an interrupt message containing the given reason to all present and
        future subscribed threads, until clear() is called."""
        if reason is None:
            reason = ''
        with self._lock:
            if self.is_set:
                raise RuntimeError('Already set. Did you forget to call clear()?')
            self.is_set = True
            self.reason = reason
            self._xpub.send(reason.encode('utf8'))

    def clear(self):
        """Cease sending interrupt messages to newly subscribed threads."""
        with self._lock:
            if not self.is_set:
                raise RuntimeError('Not set')
            self.is_set = False
            self.reason = ''


def _reraise(exc_info):
    exctype, value, traceback = exc_info
    # handle python2/3 difference in raising exception        
    if PY2:
        exec('raise exctype, value, traceback', globals(), locals())
    else:
        raise value.with_traceback(traceback)


def raise_exception_in_thread(exc_info):
    """Raises an exception in a thread"""
    threading.Thread(target=_reraise, args=(exc_info,)).start()


def start_daemon(cmd_args):
    """calls subprocess.Popen configured to detach the subprocess from the parent, such
    that it keeps running even if the parent exits. Returns None. Note that the child
    process will have its current working directory set to the value of
    tempfile.gettempdir(), rather than remaining in the parent's working directory. In
    Windows this prevents it holding a lock on the current directory, which would
    prevent it from being deleted, and the behaviour is the same on unix for
    consistency."""
    kwargs = {}
    if os.name == 'nt':
        kwargs['creationflags'] = (
            subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
        )
    else:
        childpid = os.fork()
        if childpid:
            os.waitpid(childpid, 0)
            return
        os.setsid()
    subprocess.Popen(
        cmd_args,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
        cwd=tempfile.gettempdir(),
        **kwargs
    )
    if os.name == 'posix':
        os._exit(0)


def disable_quick_edit():
    """Disable the 'quick-edit' misfeature of Windows' cmd.exe, in which a single click
    on the console puts it in 'select' mode indefinitely, causing writes to stdout from
    the program to block, freezing the program. The program remains paused like this
    even if the window is no longer focused, until the user sends a keypress to the
    console. This breaks so many things and is easy to do without realising it. This
    function disables the feature, and and adds an atexit() hook to set it back back to
    its initial configuration when Python exits.
    """
    if os.name == 'nt' and all(
        [
            a is not None and a.isatty() and a.fileno() >= 0
            for a in (sys.stdin, sys.stdout, sys.stderr)
        ]
    ):
        import win32console
        import atexit
        import pywintypes

        ENABLE_QUICK_EDIT = 0x0040
        ENABLE_EXTENDED_FLAGS = 0x0080
        console = win32console.GetStdHandle(win32console.STD_INPUT_HANDLE)
        try:
            orig_mode = console.GetConsoleMode()
        except pywintypes.error:
            # Probably there is no console after all.
            # Don't know why, but don't worry about it:
            return
        if (orig_mode & ENABLE_EXTENDED_FLAGS) and not (orig_mode & ENABLE_QUICK_EDIT):
            # Already disabled, nothing for us to do.
            return
        new_mode = (orig_mode | ENABLE_EXTENDED_FLAGS) & ~ENABLE_QUICK_EDIT
        console.SetConsoleMode(new_mode)
        atexit.register(console.SetConsoleMode, orig_mode)

        
def embed():
    # Note to self, use subprocess.CREATE_NEW_CONSOLE on Windows to ensure a new console
    # can be created.
    msg = (
        "This function no longer works with newer IPython "
        + "and I have yet to make a replacement"
    )
    raise NotImplementedError(msg)


def setup_logging(name, silent=False, directory=None):
    """Basic logging setup used by zprocess servers. silent=True will configure logging
    calls to be no-ops. directory must be specified for logging to file, otherwise only
    terminal logging will be produced. Directory will be created if it doesn't exist,
    but its parent directories must already exist"""
    LOGLEVEL = logging.DEBUG
    if directory is not None:
        logpath = os.path.join(directory, '%s.log' % name)
        if not os.path.exists(directory):
            os.mkdir(directory)
    else:
        logpath = None
    logger = logging.Logger(name)
    logger.setLevel(LOGLEVEL)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(message)s')
    file_handler_success = False
    if not silent:
        if logpath is not None:
            try:
                handler = logging.handlers.RotatingFileHandler(
                    logpath, maxBytes=50 * 1024 ** 2, backupCount=1
                )
                handler.setLevel(LOGLEVEL)
                handler.setFormatter(formatter)
                logger.addHandler(handler)
                file_handler_success = True
            except (OSError, IOError):
                file_handler_success = False
        if sys.stdout is not None and sys.stdout.isatty():
            stream_handler = logging.StreamHandler(sys.stdout)
            stream_handler.setLevel(LOGLEVEL)
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)
    if silent:
        logger.addHandler(logging.NullHandler())
    if not silent and not file_handler_success and logpath is not None:
        msg = 'Can\'t open or do not have permission to write to log file '
        msg += logpath + '. Only terminal logging will be output.'
        logger.warning(msg)
    elif file_handler_success:
        logger.info('logging to %s', logpath)
    return logger


def gethostbyname(host, prefer_ipv4=True, require_ipv4=False):
    """Like socket.gethostbyname, but may return an IPv6 address."""
    if require_ipv4:
        family = socket.AF_INET
    else:
        family = 0
    results = socket.getaddrinfo(
        host, None, family, socket.SOCK_STREAM, socket.IPPROTO_TCP
    )
    if prefer_ipv4:
        for family, _, _, _, sockaddr in results:
            if family == socket.AF_INET:
                return sockaddr[0]
    family, _, _, _, sockaddr = results[0]
    return sockaddr[0]