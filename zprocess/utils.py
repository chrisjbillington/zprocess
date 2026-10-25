import sys
import os
import signal
import threading
import logging
import logging.handlers
from binascii import hexlify
import socket
import tempfile
from pathlib import Path
import io
import subprocess

import zmq

class TimeoutError(zmq.ZMQError, TimeoutError):
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
        # subscribe() and unsubscribe() wait for their message on this socket, so it must
        # pass on every subscription and unsubscription, not only the first and last:
        self._xpub.setsockopt(zmq.XPUB_VERBOSER, 1)
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
        with self._lock:
            if not hasattr(self._local, 'sub') or not self._local.subscribed:
                raise RuntimeError('not subscribed')
            self._local.sub.unsubscribe(b'')
            # Ensure unsubscription is processed:
            self._xpub.recv()
            self._local.subscribed = False

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


def interrupt_main_thread():
    """Raise KeyboardInterrupt in the main thread, as Ctrl-C does. May be called from
    any thread. The SIGINT handler must be a Python function that raises the exception,
    such as signal.default_int_handler, which Python installs at startup unless SIGINT
    was ignored when the process started; otherwise RuntimeError is raised.

    On POSIX the signal is sent to the main thread with pthread_kill(), so a blocking
    system call in the main thread, such as time.sleep(), a lock acquisition or a zmq
    poll, returns at once and the exception is raised. On Windows the signal is raised
    in the calling thread, which sets the event that time.sleep() and, from Python
    3.14, lock acquisitions in the main thread wait on; other blocking calls are not
    woken, and the exception is raised when they return. On every platform, a call
    into C code that does not check for signals, such as a NumPy operation, is not
    interrupted, and the exception is raised when it returns."""
    handler = signal.getsignal(signal.SIGINT)
    if not callable(handler):
        msg = (
            "The SIGINT handler is not a Python function, so KeyboardInterrupt cannot "
            "be raised in the main thread. Call "
            "signal.signal(signal.SIGINT, signal.default_int_handler) in the main "
            "thread first."
        )
        raise RuntimeError(msg)
    if os.name == 'nt':
        signal.raise_signal(signal.SIGINT)
    else:
        signal.pthread_kill(threading.main_thread().ident, signal.SIGINT)


def _reraise(exc_info):
    exctype, value, traceback = exc_info
    raise value.with_traceback(traceback)


def raise_exception_in_thread(exc_info):
    """Raises an exception in a thread"""
    threading.Thread(target=_reraise, args=(exc_info,)).start()


_DAEMON_LAUNCHER = """\
import subprocess, sys
subprocess.Popen(sys.argv[1:], stderr=subprocess.DEVNULL, start_new_session=True)
"""


def start_daemon(cmd_args):
    """Start a subprocess that keeps running after the parent exits, and return None.
    The subprocess has no standard input or output, is in a new process group on
    Windows and a new session on Unix, and has the working directory
    tempfile.gettempdir(). On Windows this working directory means that the subprocess
    does not hold a lock on the parent's working directory. On Unix the subprocess is
    started by a short-lived intermediate Python process, which this function waits
    for, so the subprocess is not a child of the caller. Raises
    subprocess.CalledProcessError if the intermediate process fails."""
    kwargs = dict(
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        close_fds=True,
        cwd=tempfile.gettempdir(),
    )
    if os.name == 'nt':
        subprocess.Popen(
            cmd_args,
            stderr=subprocess.DEVNULL,
            creationflags=(
                subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
            ),
            **kwargs
        )
    else:
        subprocess.run(
            [sys.executable, '-I', '-c', _DAEMON_LAUNCHER] + list(cmd_args),
            stderr=subprocess.PIPE,
            check=True,
            **kwargs
        )


def _get_fileno(stream):
    """Return stream.get_fileno(), or -1 if the call raises io.UnsupportedOperation"""
    try:
        return stream.fileno()
    except io.UnsupportedOperation:
        # IDLE or Jupyter. Return an invalid fileno
        return -1


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
            a is not None and a.isatty() and _get_fileno(a) >= 0
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
        try:
            console.SetConsoleMode(new_mode)
        except pywintypes.error:
            # The console rejects the mode. This happens when the process was relaunched
            # from a PowerShell prompt: while PSReadLine waits for input it leaves
            # ENABLE_ECHO_INPUT set without ENABLE_LINE_INPUT, and SetConsoleMode rejects
            # any mode with that combination.
            return
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
    """Basic logging setup used by zprocess servers. silent=True sets the logger's level
    above CRITICAL, so that logging calls return without creating a record. directory
    must be specified for logging to file, otherwise only terminal logging will be
    produced. Directory will be created if it doesn't exist, but its parent directories
    must already exist"""
    LOGLEVEL = logging.DEBUG
    logger = logging.Logger(name)
    if silent:
        logger.setLevel(logging.CRITICAL + 1)
        return logger
    if directory is not None:
        logpath = os.path.join(directory, '%s.log' % name)
        if not os.path.exists(directory):
            os.mkdir(directory)
    else:
        logpath = None
    logger.setLevel(LOGLEVEL)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(message)s')
    file_handler_success = False
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
    if not file_handler_success and logpath is not None:
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


if sys.platform == 'win32':
    import ctypes
    from ctypes import wintypes

    # From mstcpip.h
    SIO_TCP_INITIAL_RTO = 0x98000011
    TCP_INITIAL_RTO_UNSPECIFIED_RTT = 0xFFFF
    TCP_INITIAL_RTO_NO_SYN_RETRANSMISSIONS = 0xFE

    class TCP_INITIAL_RTO_PARAMETERS(ctypes.Structure):
        _fields_ = [('Rtt', wintypes.USHORT), ('MaxSynRetransmissions', ctypes.c_ubyte)]

    _ws2_32 = ctypes.WinDLL('ws2_32', use_last_error=True)
    _WSAIoctl = _ws2_32.WSAIoctl
    _WSAIoctl.argtypes = [
        ctypes.c_size_t,  # SOCKET
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        ctypes.c_void_p,
        ctypes.c_void_p,
    ]
    _WSAIoctl.restype = ctypes.c_int

    def _disable_syn_retransmissions(sock):
        """Make a connection attempt on the socket fail as soon as it is refused. By
        default Windows retransmits the SYN after a refusal, so that a connection to a
        port with no listener fails only after about a second."""
        params = TCP_INITIAL_RTO_PARAMETERS(
            TCP_INITIAL_RTO_UNSPECIFIED_RTT, TCP_INITIAL_RTO_NO_SYN_RETRANSMISSIONS
        )
        returned = wintypes.DWORD()
        result = _WSAIoctl(
            sock.fileno(),
            SIO_TCP_INITIAL_RTO,
            ctypes.byref(params),
            ctypes.sizeof(params),
            None,
            0,
            ctypes.byref(returned),
            None,
            None,
        )
        if result != 0:
            raise ctypes.WinError(ctypes.get_last_error())


def port_in_use(port, timeout=1):
    """Return whether a process on this computer is listening on the TCP port. We
    attempt a connection to the port on 127.0.0.1 and close it without sending anything,
    so a ZMQServer on the port receives no message.

    On Windows the connection attempt has no timeout, because a socket timeout adds
    about 15 ms. With SYN retransmissions disabled, Windows completes or refuses a
    connection to 127.0.0.1 at once, including when the listener's backlog is full, in
    which case this returns False. On other platforms, a connection attempt that times
    out after `timeout` seconds means that the listener's backlog is full, and this
    returns True."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        if sys.platform == 'win32':
            _disable_syn_retransmissions(sock)
        else:
            sock.settimeout(timeout)
        try:
            sock.connect(('127.0.0.1', port))
        except ConnectionRefusedError:
            return False
        except socket.timeout:
            return True
        return True


def get_venv_executable_and_env(env=None):
    """Return the executable with which to start a Python subprocess, and the
    environment to pass to it. On Windows in a venv, sys.executable is a launcher that
    runs sys._base_executable as a child process; to skip that extra process, return
    sys._base_executable and an environment with __PYVENV_LAUNCHER__ set, which tells
    it to configure itself for the venv, as the launcher would. Otherwise return
    sys.executable and env as passed in. If env is None and needs to be modified for the
    subprocess, a copy of os.environ, appropriately modified, is returned.

    On macOS the venv's python is either a symlink to the framework's bin/python3.x
    stub or a copy of it, and the stub execs the interpreter with __PYVENV_LAUNCHER__
    set to its own path, so there is no extra process to skip and running
    sys._base_executable would give a subprocess that does not see the venv."""
    base_executable = getattr(sys, '_base_executable', sys.executable)
    if os.name == 'nt' and Path(base_executable) != Path(sys.executable):
        if env is None:
            env = os.environ.copy()
        env["__PYVENV_LAUNCHER__"] = str(sys.executable)
        return base_executable, env
    return sys.executable, env

