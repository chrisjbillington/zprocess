from __future__ import unicode_literals, print_function, division
import sys
import os
PY2 = sys.version_info.major == 2
if PY2:
    str = unicode
if PY2:
    import subprocess32 as subprocess
    from time import time as monotonic
else:
    import subprocess
    from time import monotonic

from weakref import WeakSet
import atexit
from zprocess import ZMQServer
from zprocess.utils import setup_logging
from zprocess.remote import PROTOCOL_VERSION

ERR_INVALID_COMMAND = 'error: invalid command'
ERR_NO_SUCH_PROCESS = 'error: no such process'

logger = None

# Some machinery to terminate running processes upon interpreter shutdown:

_all_children = WeakSet()

def _atexit_cleanup():
    while True:
        try:
            child = _all_children.pop()
        except KeyError:
            break
        try:
            child.terminate()
            try:
                child.wait(1)
            except subprocess.TimeoutExpired:
                child.kill()
        except OSError:
            pass  # process is already dead

_atexit_registered = False


# How long to keep info about dead processes in memory after they have exited if the
# parent has not called __del__:
CLEANUP_INTERVAL = 60


class RemoteProcessServer(ZMQServer):
    def __init__(
        self,
        port=None,
        bind_address='tcp://0.0.0.0',
        shared_secret=None,
        allow_insecure=True,
        silent=False,
    ):
        # Entries should be removed from this dict if the parent calls __del__ on
        # the proxy, or if the child dies for some other reason.
        self.children = {}
        # IP address from which the request for each child process came:
        self.parents = {}
        # Children whose parents have called __del__ but which are still alive:
        self.orphans = set()
        # PIDs of children who have terminated (either by themselves or on request from
        # their parent) but whose parents have not called __del__, mapped to the time
        # at which we noticed they terminated:
        self.time_of_termination = {}

        ZMQServer.__init__(
            self,
            port=port,
            bind_address=bind_address,
            shared_secret=shared_secret,
            allow_insecure=allow_insecure,
            timeout_interval=1,
        )
        global logger
        logger = setup_logging('zprocess-remote', silent)
        if not silent:
            self.sock.logger = logger
        msg = 'This is zprocess-remote server, running on %s:%d'
        logger.info(msg, self.bind_address, self.port)
        # If this is the first server being started in this process, register our atexit
        # cleanup function:
        global _atexit_registered
        if not _atexit_registered:
            atexit.register(_atexit_cleanup)
            _atexit_registered = True

    def timeout(self):
        # Poll orphans so we can delete them if they are closed
        for pid in self.orphans.copy():
            rc = self.children[pid].poll()
            if rc is not None:
                logger.info('orphan %d exited', pid)
                # Child is dead, clean up:
                _all_children.remove(self.children[pid])
                del self.children[pid]
                del self.parents[pid]
                self.orphans.remove(pid)
        now = monotonic()
        # Poll other processes to see if they have unexpectedly closed:
        for pid in self.children:
            if pid not in self.time_of_termination:
                rc = self.children[pid].poll()
                if rc is not None:
                    logger.info(
                        'child %d exited of its own accord with status %d', pid, rc
                    )
                    self.time_of_termination[pid] = now
        # Clean up details of processes that have been dead for longer than
        # CLEANUP_INTERVAL without the parent calling __del__:
        for pid in self.time_of_termination.copy():
            if now > self.time_of_termination[pid] + CLEANUP_INTERVAL:
                # Clean up:
                logger.info('Cleaning up child %d; parent never called __del__', pid)
                _all_children.remove(self.children[pid])
                del self.children[pid]
                del self.parents[pid]
                del self.time_of_termination[pid]


    def proxy_terminate(self, pid):
        return self.children[pid].terminate()

    def proxy_kill(self, pid):
        return self.children[pid].kill()

    def proxy_wait(self, pid):
        # We only wait for 10ms - the client can implement a blocking wait by
        # calling multiple times, we don't want to be blocked here:
        try:
            return self.children[pid].wait(0.01)
        except subprocess.TimeoutExpired:
            return None

    def proxy_poll(self, pid):
        return self.children[pid].poll()

    def proxy___del__(self, pid):
        try:
            child = self.children[pid]
        except KeyError:
            # Probably already dead and cleaned up earlier. Nothing to do here.
            return
        rc = child.poll()
        if rc is None:
            # Process still running, but deleted by parent. Mark it as an orphan for
            # later cleanup
            logger.info('%d is an orphan', pid)
            self.orphans.add(pid)
        else:
            _all_children.remove(self.children[pid])
            del self.children[pid]
            del self.parents[pid]
            if pid in self.time_of_termination:
                del self.time_of_termination[pid]

    def proxy_Popen(self, cmd, *args, **kwargs):
        if kwargs.pop('prepend_sys_executable', False):
            cmd = [sys.executable] + cmd
        # Update current environment with the contents of 'extra_env', if given:
        kwargs['env'] = os.environ.copy()
        kwargs['env'].update(kwargs.pop('extra_env', {}))
        if any(kwarg in kwargs for kwarg in ['stdout', 'stdin', 'stderr']):
            msg = "Cannot specify stdout, stdin or stderr for remote process."
            raise ValueError(msg)
        kwargs['stdout'] = kwargs['stdin'] = kwargs['stderr'] = subprocess.DEVNULL
        child = subprocess.Popen(cmd, *args, **kwargs)
        self.children[child.pid] = child
        _all_children.add(child)
        self.parents[child.pid] = self.sock.peer_ip
        return child.pid

    def handler(self, data):
        command, args, kwargs = data
        if hasattr(self, 'proxy_' + command):
            if not args:
                logger.info('%s: invalid command', self.sock.peer_ip)
                return ERR_INVALID_COMMAND
            # Check valid pid:
            if command != 'Popen':
                pid = args[0]
                if pid not in self.children:
                    logger.info(
                        '%s: %s: no such process %s', self.sock.peer_ip, command, pid
                    )
                    return ERR_NO_SUCH_PROCESS
                logger.info('%s: %s %s', self.sock.peer_ip, command, pid)
            else:
                logger.info('%s: %s', self.sock.peer_ip, command)
            return getattr(self, 'proxy_' + command)(*args, **kwargs)
        elif command == 'whoami':
            # Client is requesting its IP address from our perspective
            logger.info('%s: %s', self.sock.peer_ip, command)
            return self.sock.peer_ip
        elif command == 'hello':
            logger.info('%s: %s', self.sock.peer_ip, command)
            return 'hello'
        elif command == 'protocol':
            logger.info('%s: %s', self.sock.peer_ip, command)
            return PROTOCOL_VERSION
        else:
            logger.info('%s: invalid command', self.sock.peer_ip)
            return ERR_INVALID_COMMAND