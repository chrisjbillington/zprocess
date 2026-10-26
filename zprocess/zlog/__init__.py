import sys
import os
import socket
import threading
from logging import Handler
import uuid

try:
    import builtins
except ImportError:
    import __builtin__ as builtins

from time import monotonic
import zmq
from zprocess.security import SecureContext

DEFAULT_PORT = 7340

_zmq_log_client = None


def dedent(s):
    return ' '.join(s.split())


class ZLogClient(object):

    RESPONSE_TIMEOUT = 5000

    def __init__(
        self,
        host='localhost',
        port=DEFAULT_PORT,
        shared_secret=None,
        allow_insecure=False,
    ):
        self.host = socket.gethostbyname(host)
        self.port = port
        self.shared_secret = shared_secret
        self.allow_insecure = allow_insecure
        # We'll store one zmq socket for each thread, with thread local storage:
        self.local = threading.local()
        self.suppress_further_warnings = False

    def _new_socket(self, timeout=None):
        """Create a socket for this thread and connect to the server with a timeout in
        seconds"""
        # We have a separate push socket for each thread that needs to push.
        # convert timeout to ms or use default:
        timeout = self.RESPONSE_TIMEOUT if timeout is None else 1000 * timeout
        context = SecureContext.instance(shared_secret=self.shared_secret)
        self.local.sock = context.socket(zmq.DEALER, allow_insecure=self.allow_insecure)
        try:
            self.local.sock.setsockopt(zmq.LINGER, 0)
            self.local.sock.set_hwm(1000)
            self.local.sock.connect(
                'tcp://%s:%s' % (self.host, str(self.port)), timeout=timeout
            )
            self.local.poller = zmq.Poller()
            self.local.poller.register(self.local.sock, zmq.POLLIN)
        except Exception:
            # Do not keep the socket:
            del self.local.sock
            raise

    def handler(self, filepath):
        """Return a logging handler configured to communicate with this server"""
        return ZMQLoggingHandler(self, filepath)

    def _send(self, *messages, **kwargs):
        """Create socket if needed and send messages. timeout used for connection
        timeout in the case that a new socket is made. timeout in seconds."""
        # convert timeout to ms if needed
        timeout = kwargs.get('timeout', None)
        if not hasattr(self.local, 'sock'):
            self._new_socket(timeout)
        self.local.sock.send_multipart([b''] + list(messages), zmq.NOBLOCK)

    def _recv(self, timeout=None):
        """Receive with a timeout in seconds"""
        # convert timeout to ms or use default:
        timeout = self.RESPONSE_TIMEOUT if timeout is None else 1000 * timeout
        try:
            events = self.local.poller.poll(timeout)
            if not events:
                raise zmq.ZMQError('No response from zlog server: timed out')
            response = self.local.sock.recv_multipart()
            if len(response) != 2 or response[0] != b'':
                raise zmq.ZMQError('Malformed message from server: ' + str(response))
            if response[1].startswith(b'error:'):
                raise ValueError(response[1].decode('utf8'))
            return response[1]
        except:
            self.local.sock.close(linger=0)
            del self.local.sock
            del self.local.poller
            raise

    def ping(self, timeout=None):
        """Ping the server for a response. Timeout in seconds"""
        start_time = monotonic()
        self._send(b'hello', timeout=timeout)
        response = self._recv(timeout).decode('utf8')
        if response == 'hello':
            return round((monotonic() - start_time) * 1000, 2)
        raise zmq.ZMQError('Invalid response from server: ' + response)

    def get_protocol_version(self, timeout=None):
        """Ask the server what protocol version it is running"""
        self._send(b'protocol', timeout=timeout)
        return self._recv(timeout).decode('utf8')

    def check_access(self, filepath, timeout=None):
        """Send a message to the logging server, asking it to check that it can open the
        log file in append mode. Raises an exception if the file cannot be opened."""
        if not isinstance(filepath, bytes):
            filepath = filepath.encode('utf8')
        self._send(b'check_access', filepath, timeout=timeout)
        response = self._recv(timeout).decode('utf8')
        if response == 'ok':
            return
        # Raise the exception returned by the server:
        try:
            exc_class_name, message = response.split(': ', 1)
            exc_class = getattr(builtins, exc_class_name, OSError)
        except ValueError:
            exc_class = OSError
            message = response
        if not issubclass(exc_class, OSError):
            exc_class = OSError
        raise exc_class(message)

    def log(self, client_id, filepath, message):
        """Send a message to the logging server, asking it to write it to the specified
        filepath"""
        if not isinstance(filepath, bytes):
            filepath = filepath.encode('utf8')
        if not isinstance(message, bytes):
            message = message.encode('utf8')
        try:
            self._send(b'log', client_id, filepath, message)
        except zmq.Again:
            if not self.suppress_further_warnings:
                self.suppress_further_warnings = True
                msg = """Warning: zlog server not receiving log messages. Logging may
                    not be functional\n"""
                sys.stderr.write(dedent(msg))

    def done(self, client_id, filepath, timeout=None):
        """Tell the server a client is done with the file"""
        if not isinstance(filepath, bytes):
            filepath = filepath.encode('utf8')
        self._send(b'done', client_id, filepath, timeout=timeout)
        response = self._recv(timeout).decode('utf8')
        if response != 'ok':
            raise zmq.ZMQError('Invalid response from server: ' + response)


class ZMQLoggingHandler(Handler):
    """Logging handler that sends log messages to a zlog server"""

    def __init__(self, zlog_client, filepath):
        self.client = zlog_client
        self.filepath = os.path.abspath(filepath)
        # A unique ID so that the server can identify us:
        self.client_id = uuid.uuid4().hex.encode('utf8')
        Handler.__init__(self)
        self.client.check_access(self.filepath)

    def close(self):
        """Tell the server we're done with the file. It will know to close the file once
        all clients are done with it."""
        Handler.close(self)
        self.client.done(self.client_id, self.filepath)

    def emit(self, record):
        """Format and send the record to the server"""
        msg = self.format(record)
        self.client.log(self.client_id, self.filepath, msg)
