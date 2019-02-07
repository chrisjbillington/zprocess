from __future__ import unicode_literals, print_function, division
import sys
PY2 = sys.version_info.major == 2
if PY2:
    str = unicode
import os
if PY2:
    import subprocess32 as subprocess
else:
    import subprocess
import logging
import logging.handlers

if __package__ is None:
    sys.path.insert(0, os.path.abspath('../..'))

import zprocess
from zprocess import ZMQServer
from zprocess.utils import setup_logging
from zprocess.remote import PROTOCOL_VERSION

ERR_INVALID_COMMAND = 'error: invalid command'
ERR_NO_SUCH_PROCESS = 'error: no such process'


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

        ZMQServer.__init__(
            self,
            port=port,
            bind_address=bind_address,
            shared_secret=shared_secret,
            allow_insecure=allow_insecure,
            timeout_interval=1,
        )
        setup_logging('zprocess-remote', silent)
        if not silent:
            self.sock.logger = logging.getLogger()
        msg = 'This is zprocess-remote server, running on %s:%d'
        logging.info(msg, self.bind_address, self.port)

    def timeout(self):
        # Poll orphans so we can delete them if they are closed
        for pid in self.orphans.copy():
            rc = self.children[pid].poll()
            if rc is not None:
                logging.info('orphan %d exited', pid)
                # Child is dead, clean up:
                del self.children[pid]
                del self.parents[pid]
                self.orphans.remove(pid)

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

    def proxy_returncode(self, pid):
        return self.children[pid].returncode

    def proxy___del__(self, pid):
        child = self.children[pid]
        rc = child.poll()
        if rc is None:
            # Process still running, but deleted by parent. Mark it as an orphan for
            # later cleanup
            logging.info('%d is an orphan', pid)
            self.orphans.add(pid)
        else:
            del self.children[pid]
            del self.parents[pid]

    def proxy_Popen(self, cmd, *args, **kwargs):
        if kwargs.pop('prepend_sys_executable', False):
            cmd = [sys.executable] + cmd
        if any(kwarg in kwargs for kwarg in ['stdout', 'stdin', 'stderr']):
            msg = "Cannot specify stdout, stdin or stderr for remote process."
            raise ValueError(msg)
        kwargs['stdout'] = kwargs['stdin'] = kwargs['stderr'] = subprocess.DEVNULL
        child = subprocess.Popen(cmd, *args, **kwargs)
        self.children[child.pid] = child
        self.parents[child.pid] = self.sock.peer_ip
        return child.pid

    def handler(self, data):
        command, args, kwargs = data
        logging.info('%s: %s', self.sock.peer_ip, command)
        if hasattr(self, 'proxy_' + command):
            if not args:
                return ERR_INVALID_COMMAND
            # Check valid pid:
            if command != 'Popen':
                pid = args[0]
                if pid not in self.children:
                    return ERR_NO_SUCH_PROCESS
            return getattr(self, 'proxy_' + command)(*args, **kwargs)
        elif command == 'whoami':
            # Client is requesting its IP address from our perspective
            return self.sock.peer_ip
        elif command == 'hello':
            return 'hello'
        elif command == 'protocol':
            return PROTOCOL_VERSION
        else:
            return ERR_INVALID_COMMAND
