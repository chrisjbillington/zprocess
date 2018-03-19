# What does the command server need to do? It needs to start up and start listening
# publicly on the command server port, and the heartbeating port.

# when it recieves a command, it should launch a subprocess. How does it know what
# the ports are? Maybe the ports are JSON, and prefixed by --zprocess_parentinfo,
# for backward compatibility and to allow other args to be passed into the program.

# For each parent heartbeating server, it will have a HeartBeatProxy, which is both
# a heartbeat server and client.
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
print(sys.path[0])
print(zprocess.__file__)


class RemoteProcessServer(ZMQServer):
    DEFAULT_PORT = 7340
    def __init__(self, port, bind_address='tcp://0.0.0.0', shared_secret=None,
                 allow_insecure=True):
        ZMQServer.__init__(self, port, bind_address=bind_address,
                           shared_secret=shared_secret,
                           allow_insecure=allow_insecure)
        # Entries should be removed from this dict if the parent calls __del__ on
        # the proxy, or if the parent stops responding to heartbeats:
        self.children = {}

    def proxy_terminate(self, pid):
        return self.children[pid].terminate()

    def proxy_kill(self, pid):
        return self.children[pid].kill()

    def proxy_wait(self, pid):
        # We only wait for 10ms - the client can implement a blocking wait by
        # calling multiple times, we don't want to be blocked here:
        return self.children[pid].wait(0.01)

    def proxy_poll(self, pid):
        return self.children[pid].poll()

    def proxy_returncode(self):
        return self.client.returncode

    def proxy___del__(self, pid):
        del self.children[pid]

    def proxy_Popen(self, args):
        child = subprocess.Popen(args)
        self.children[child.pid] = child
        return child.pid

    def handler(self, data):
        command, args, kwargs = data
        print(command, args, kwargs)
        return getattr(self, 'proxy_' + command)(*args, **kwargs)


if __name__ == '__main__':
    print("Starting RemoteProcessServer on port", RemoteProcessServer.DEFAULT_PORT)
    server = RemoteProcessServer(RemoteProcessServer.DEFAULT_PORT)
    print('Press Ctrl-C to shutdown')
    server.shutdown_on_interrupt()