from __future__ import division, unicode_literals, print_function, absolute_import
import sys
PY2 = sys.version_info.major == 2
import os
if PY2:
    import subprocess32 as subprocess
else:
    import subprocess

_splitcwd = os.getcwd().split(os.path.sep)
_path = os.path.join(*_splitcwd[:-2])
_cwd = os.path.join(*_splitcwd[-2:])

if _cwd == os.path.join('zprocess', 'remote') and _path not in sys.path:
    # Running from within zprocess dir? Add to sys.path for testing during
    # development:
    print(_path)
    sys.path.insert(0, _path)


import zprocess
from zprocess import ZMQServer


class RemoteProcessServer(ZMQServer):
    DEFAULT_PORT = 7341
    def __init__(self, port, bind_address='tcp://0.0.0.0', shared_secret=None,
                 allow_insecure=True):
        ZMQServer.__init__(self, port, bind_address=bind_address,
                           shared_secret=shared_secret,
                           allow_insecure=allow_insecure)
        # Entries should be removed from this dict if the parent calls __del__ on
        # the proxy, or if the child dies for some other reason.
        self.children = {}

    def proxy_terminate(self, pid):
        return self.children[pid].terminate()

    def proxy_kill(self, pid):
        return self.children[pid].kill()

    def proxy_wait(self, pid):
        # We only wait for 10ms - the client can implement a blocking wait by
        # calling multiple times, we don't want to be blocked here:
        try:
            self.children[pid].wait(0.01)
        except subprocess.TimeoutExpired:
            return None

    def proxy_poll(self, pid):
        return self.children[pid].poll()

    def proxy_returncode(self):
        return self.client.returncode

    def proxy___del__(self, pid):
        del self.children[pid]

    def proxy_Popen(self, cmd, *args, **kwargs):
        cmd, kwargs = args
        if kwargs.pop('prepend_sys_executable', False):
            cmd = [sys.executable] + cmd
        child = subprocess.Popen(cmd, *args, **kwargs)
        self.children[child.pid] = child
        return child.pid

    def handler(self, data):
        command, args, kwargs = data
        print(command, args, kwargs)
        if hasattr(self, 'proxy_' + command):
            return getattr(self, 'proxy_' + command)(*args, **kwargs)
        elif command == 'whoami':
            # Client is requesting its IP address from our perspective
            return self.sock.peer_ip


if __name__ == '__main__':
    print("Starting RemoteProcessServer on port", RemoteProcessServer.DEFAULT_PORT)
    server = RemoteProcessServer(RemoteProcessServer.DEFAULT_PORT)
    print('Press Ctrl-C to shutdown')
    server.shutdown_on_interrupt()