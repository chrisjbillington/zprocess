from __future__ import unicode_literals, print_function, division
import sys
import time
PY2 = sys.version_info.major == 2
if PY2:
    str = unicode
    from time import time as monotonic
else:
    from time import monotonic
import zprocess

DEFAULT_PORT = 7341
PROTOCOL_VERSION = '1.0.0'

from zprocess import ZMQServer


class RemoteChildProxy(object):
    def __init__(self, remote_process_client, pid):
        """Class to wrap operations on a remote subprocess. Extra keyword arguments to
        any method will be forwarded to the underlying ZMQClient.get() method used for
        communicating with the remote process server, notably `interruptor` and
        `timeout`. To be passed to the get() call, these keyword arguments must be
        prefixed with `get_`, i.e. `get_timeout`, `get_interrupt`. This is to
        disambiguate them from keyword arguments to methods of Popen (at least one of
        which is `timeout`).
        """
        self.client = remote_process_client
        self.pid = pid
        self.returncode = None

    def request(self, funcname, *args, **kwargs):
        return self.client.request(funcname, self.pid, *args, **kwargs)

    def terminate(self, **kwargs):
        return self.request('terminate', **kwargs)

    def kill(self, **kwargs):
        return self.request('kill', **kwargs)

    def wait(self, timeout=None, **kwargs):
        # The server will only do 0.01 second timeouts at a time to not be blocked from
        # other requests, so we will make requests at 0.1 second intervals to reach
        # whatever the requested timeout was:
        if timeout is not None:
            deadline = monotonic() + timeout
        while True:
            result = self.request('wait', **kwargs)
            self.returncode = result
            if result is not None or (timeout is not None and monotonic() > deadline):
                return result
            time.sleep(0.1)

    def poll(self, **kwargs):
        self.returncode = self.request('poll', **kwargs)
        return self.returncode

    def __del__(self):
        try:
            # Short timeout so as not to block since __del__ could be called from
            # anywhere. Can't do much if __del__ fails in any case.
            self.request('__del__', get_timeout=0.2)
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
        get_kwargs = {}
        for kwarg in kwargs.copy():
            if kwarg.startswith('get_'):
                get_kwargs[kwarg.split('get_')[1]] = kwargs.pop(kwarg)
        return self.get(
            self.port, self.host, data=[command, args, kwargs], **get_kwargs
        )

    def say_hello(self, **kwargs):
        return self.request('hello', **kwargs)

    def Popen(self, cmd, *args, **kwargs):
        """Launch a remote process and return a proxy object for interacting with it. If
        prepend_sys_executable=True, command will have sys.executable prefixed to it on
        the server before running."""
        kwargs.setdefault('prepend_sys_executable', False)
        pid = self.request('Popen', cmd, *args, **kwargs)
        return RemoteChildProxy(self, pid)

    def get_external_IP(self, **kwargs):
        """Ask the RemoteProcessServer what our IP address is from its perspective"""
        return self.request('whoami', **kwargs)

    def get_protocol(self, **kwargs):
        return self.request('protocol', **kwargs)


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
            sys.stderr.flush()
        else:
            if PY2:
                sys.stdout.write(text)
            else:
                sys.stdout.buffer.write(text)  # Since it is binary data
            sys.stdout.flush()