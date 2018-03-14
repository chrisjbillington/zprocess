from __future__ import division, unicode_literals, print_function, absolute_import
import sys
import os
import threading
import subprocess
import time

import zmq
from zmq.utils.monitor import recv_monitor_message

PY2 = sys.version_info[0] == 2
if PY2:
    str = unicode


class TimeoutError(zmq.ZMQError):
    pass


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
    """calls subprocess.Popen configured to detach the subprocess from the
    parent, such that it keeps running even if the parent exits. Returns None.
    Note that the child process will have its current working directory set to
    the value of tempfile.gettempdir(), rather than remaining in the parent's
    working directory. In Windows this prevents it holding a lock on the
    current directory, which would prevent it from being deleted, and the
    behaviour is the same on unix for consistency."""
    import tempfile
    if os.name == 'nt':
        creationflags=0x00000008 # DETACHED_PROCESS from the win32 API
        subprocess.Popen(cmd_args,
                         creationflags=creationflags, stdout=None, stderr=None,
                         close_fds=True, cwd=tempfile.gettempdir())
    else:
        devnull = open(os.devnull,'w')
        if not os.fork():
            os.setsid()
            subprocess.Popen(cmd_args,
                             stdin=devnull, stdout=devnull, stderr=devnull,
                             close_fds=True, cwd=tempfile.gettempdir())
            os._exit(0)


def embed():
    """embeds an IPython qt console in the calling scope.
    Intended for debugging. May cause strange interpreter behaviour."""

    # Imports, including ones we only need in the qtconsole process,
    # so that the user gets errors about them rather than just no qtconsole:

    from IPython.utils.frame import extract_module_locals
    from ipykernel.kernelapp import IPKernelApp
    from IPython.core.interactiveshell import InteractiveShell


    from zmq.eventloop import ioloop


    def launch_qtconsole():
        subprocess.call([sys.executable, '-c',
                        'from qtconsole.qtconsoleapp import main; main()',
                        '--existing', app.connection_file])
        if not kernel_has_quit.is_set():
            ioloop.IOLoop.instance().stop()

    kernel_has_quit = threading.Event()
    qtconsole_thread = threading.Thread(target=launch_qtconsole)
    qtconsole_thread.daemon = True


    # Hack to prevent the kernel app from disabline SIGINT:
    IPKernelApp.init_signal = lambda self: None

    # Get some interpreter state that will need to be restored after the
    # kernel quits:
    sys_state = sys.stdin, sys.stdout, sys.stderr, sys.displayhook, sys.excepthook
    ps1 = getattr(sys, 'ps1', None)
    ps2 = getattr(sys, 'ps2', None)
    ps3 = getattr(sys, 'ps3', None)

    # Some of the below copied from ipykernel.embed.embed_kernel
    app = IPKernelApp()
    app.initialize([])

    # Remove the exit handler, we'll run it manually rather than at
    # interpreter exit:
    # atexit.unregister(app.kernel.shell.atexit_operations)

    # Undo unnecessary sys module mangling from init_sys_modules.
    # This would not be necessary if we could prevent it
    # in the first place by using a different InteractiveShell
    # subclass, as in the regular embed case.
    main = app.kernel.shell._orig_sys_modules_main_mod
    if main is not None:
        sys.modules[app.kernel.shell._orig_sys_modules_main_name] = main

    # load the calling scope if not given
    (caller_module, caller_locals) = extract_module_locals(1)
    app.kernel.user_module = caller_module
    app.kernel.user_ns = caller_locals
    app.shell.set_completer_frame()

    qtconsole_thread.start()
    try:
        app.start()
    finally:
        sys.stdin, sys.stdout, sys.stderr, sys.displayhook, sys.excepthook = sys_state
        if ps1 is not None:
            sys.ps1 = ps1
        else:
            del sys.ps1
        if ps2 is not None:
            sys.ps2 = ps2
        else:
            del sys.ps2
        if ps3 is not None:
            sys.ps3 = ps3
        else:
            del sys.ps3
        kernel_has_quit.set()
