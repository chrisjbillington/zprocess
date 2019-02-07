from __future__ import unicode_literals, print_function, division
import sys
PY2 = sys.version_info.major == 2
if PY2:
    str = unicode
import zprocess
import time

DEFAULT_PORT = 7341
PROTOCOL_VERSION = '1.0.0'

from zprocess import ZMQServer


class RemoteChildProxy(object):
    def __init__(self, remote_process_client, pid):
        """Class to wrap operations on a remote subprocess"""
        self.client = remote_process_client
        self.pid = pid

    def request(self, funcname, *args, **kwargs):
        return self.client.request(funcname, self.pid, *args, **kwargs)

    def terminate(self):
        return self.request('terminate')

    def kill(self):
        return self.request('kill')

    def wait(self, timeout=None):
        # The server will only do 0.01 second timeouts at a time to not be blocked
        # from other requests, so we will make requests at 0.1 second intervals to
        # reach whatever the requested timeout was:
        if timeout is not None:
            end_time = time.time() + timeout
        while True:
            result = self.request('wait')
            if result is not None or (timeout is not None and time.time() > end_time):
                return result
            time.sleep(0.1)

    def poll(self):
        return self.request('poll')

    @property
    def returncode(self):
        return self.request('returncode')

    def __del__(self):
        try:
            self.request('__del__')
        except Exception:
            pass


class RemoteProcessClient(zprocess.clientserver.ZMQClient):
    """A class to represent communication with a RemoteProcessServer"""

    def __init__(
        self, host, port=DEFAULT_PORT, shared_secret=None, allow_insecure=False
    ):
        zprocess.clientserver.ZMQClient.__init__(
            self, shared_secret=shared_secret, allow_insecure=allow_insecure
        )
        self.shared_secret = shared_secret
        self.host = host
        self.port = port

    def request(self, command, *args, **kwargs):
        return self.get(self.port, self.host, data=[command, args, kwargs], timeout=5)

    def say_hello(self):
        return self.request('hello')

    def Popen(self, cmd, *args, **kwargs):
        """Launch a remote process and return a proxy object for interacting with it. If
        prepend_sys_executable=True, command will have sys.executable prefixed to it on
        the server before running."""
        kwargs.setdefault('prepend_sys_executable', False)
        pid = self.request('Popen', cmd, *args, **kwargs)
        return RemoteChildProxy(self, pid)

    def get_external_IP(self):
        """Ask the RemoteProcessServer what our IP address is from its perspective"""
        return self.request('whoami')

    def get_protocol(self):
        return self.request('protocol')


class RemoteOutputReceiver(ZMQServer):
    """Class to recieve stdout and stderr from remote subprocesses that do not otherwise
    have output redirection configured, and print them to our stdout/stderr."""

    def __init__(
        self,
        port=None,
        bind_address='tcp://0.0.0.0',
        shared_secret=None,
        allow_insecure=False,
    ):
        ZMQServer.__init__(
            self,
            port=port,
            dtype='multipart',
            pull_only=True,
            bind_address=bind_address,
            shared_secret=shared_secret,
            allow_insecure=allow_insecure,
        )

    def handler(self, data):
        charformat_repr, text = data
        # Print stderr to stderr and anything else to stdout regardless of
        # the charformat
        if charformat_repr == b'stderr':
            if PY2:
                sys.stderr.write(text)
            else:
                sys.stderr.buffer.write(text)  # Since it is binary data
        else:
            if PY2:
                sys.stdout.write(text)
            else:
                sys.stdout.buffer.write(text)  # Since it is binary data
