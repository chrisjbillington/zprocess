from __future__ import print_function, unicode_literals, division
import sys
import ctypes
import base64
import weakref
import time

import ipaddress
import zmq
import zmq.auth.thread
from zmq.utils.monitor import recv_monitor_message


if sys.version_info[0] == 2:
    str = unicode

def _check_versions():
    if not zmq.zmq_version_info() >= (4, 0, 0):
        raise ImportError('Require libzmq >= 4.0')

    _libzmq = ctypes.CDLL(zmq.backend.cython.utils.__file__)
    if not hasattr(_libzmq, 'sodium_init'):
        msg = ('zprocess warning: libzmq not built with libsodium. ' +
               'Encryption/decryption will be slow. If on Windows, ' +
               'use conda zeromq/pyzmq packages for fast crypto.\n')
        sys.stderr.write(msg)

    if not hasattr(zmq, 'curve_public'):
        # Access the function via ctypes if not in pyzmq:
        if hasattr(_libzmq, 'zmq_curve_public'):
            # Use the zeromq function
            def _curve_public(secret_key):
                public_key = b'0' * 40
                zmq.error._check_rc(_libzmq.zmq_curve_public(public_key,
                                                             secret_key))
                return public_key
        else:
            # Old zeromq, use its crypto library function directly:
            if hasattr(_libzmq, 'crypto_scalarmult_base'):
                def _curve_public(secret_key):
                    public_key_bytes = b'0' * 40
                    secret_key_bytes = zmq.utils.z85.decode(secret_key)
                    _libzmq.crypto_scalarmult_base(public_key_bytes,
                                                   secret_key_bytes)
                    return zmq.utils.z85.encode(public_key_bytes[:32])
            else:
                # Old zeromq, and not built with libsodium. We can't proceed.
                msg = ("Require zeromq >= 4.2.0, " +
                       "or zeromq >= 4.0.0 built with libsodium")
                raise ImportError(msg)
        zmq.curve_public = _curve_public


# Constants in zeromq 4.3.0 but not yet present in pyzmq at tine of writing:
zmq.EVENT_HANDSHAKE_FAILED_NO_DETAIL = 0x0800
zmq.EVENT_HANDSHAKE_SUCCEEDED = 0x1000
zmq.EVENT_HANDSHAKE_FAILED_PROTOCOL = 0x2000
zmq.EVENT_HANDSHAKE_FAILED_AUTH = 0x4000


if zmq.zmq_version_info() >= (4, 3, 0):
    # Events to tell when a conneciton has succeeded, including authentication:
    CONN_SUCCESS_EVENTS = {zmq.EVENT_HANDSHAKE_SUCCEEDED}
    CONN_FAIL_EVENTS = {
        zmq.EVENT_HANDSHAKE_FAILED_NO_DETAIL,
        zmq.EVENT_HANDSHAKE_FAILED_PROTOCOL,
        zmq.EVENT_HANDSHAKE_FAILED_AUTH,
    }
else:
    # Authentication failure is not detectable on zmq < 4.3. Failure will just look like
    # success. Too bad.
    CONN_SUCCESS_EVENTS = {zmq.EVENT_CONNECTED}
    CONN_FAIL_EVENTS = set()


class InsecureConnection(RuntimeError):
    pass


class AuthenticationFailure(RuntimeError):
    pass


INSECURE_ERROR = ' '.join(
"""Plaintext socket send() or recv() on external interface. This can allow an
attacker arbitrary code execution if you receive and unpickle Python objects,
and open your application to other attacks. Unless your network is fully
trusted, pass a preshared secret generated by generate_shared_secret() to the
SecureContext() to secure your connection, or bind only on the internal
network interface if communication is internal. To proceed insecurely at your
own risk, use to SecureContext.socket(sockettype,
allow_insecure=True)""".splitlines())


def generate_shared_secret():
    """Compute a new pair of random CurveZMQ secret keys, decode them from z85
    encoding, and return the result as a base64 encoded unicode string as suitable
    for passing to SecureContext() or storing on disk. We use base64 because it is
    more compatible with Python config files than z85."""
    _check_versions()
    _, client_secret = zmq.curve_keypair()
    _, server_secret = zmq.curve_keypair()
    return base64.b64encode(zmq.utils.z85.decode(client_secret) + 
                            zmq.utils.z85.decode(server_secret)).decode('utf8')

def _unpack_shared_secret(shared_secret):
    """Base64 decode, split and z85 encode the shared secret to produce the
    client and server CurveZMQ secret keys."""
    binary_secret = base64.b64decode(shared_secret)
    if not len(binary_secret) == 64:
        msg = 'Shared secret should be 64 bytes, got %d' % len(binary_secret)
        raise ValueError(msg)
    client_secret, server_secret = binary_secret[:32], binary_secret[32:]
    return zmq.utils.z85.encode(client_secret), zmq.utils.z85.encode(server_secret)


class SecureSocket(zmq.Socket):
    """A Socket that configures as a CurveZMQ server upon bind() and as a CurveZMQ
    client upon connect(), using the keys held by the parent SecureContext to
    authenticate and be authenticated by its peer. If the shared_secret passed to the
    parent SecureContext() was None, then plaintext sockets will be used, but
    InsecureConnection will be raised if send() or recv() are called when bound or
    connected to an external network interface. This can be suppressed by passing
    allow_insecure=True to the Context.socket() call. inproc:// connections are not
    secured. For TCP connections, the IP address of the sender is saved as self.peer_ip
    after each message is received."""

    # Dummy class attrs to distinguish from zmq options:
    secure = None
    allow_insecure = None
    peer_ip = None
    tcp = None

    def __init__(self, *args, **kwargs):
        self.allow_insecure = kwargs.pop('allow_insecure', False)
        zmq.Socket.__init__(self, *args, **kwargs)
        # The IP address, if any, of the peer we last received a message from:
        self.peer_ip = None
        # Whether the socket is using tcp transport:
        self.tcp = False

    def _is_internal(self, endpoint):
        """Return whether a bind or connect endpoint is on an internal
        interface"""
        import socket
        if endpoint.startswith('inproc://'):
            return True
        if endpoint.startswith('tcp://'):
            host = ''.join(''.join(endpoint.split('//')[1:]).split(':')[0])
            if host == '*':
                return False
            address = socket.gethostbyname(host)
            if isinstance(address, bytes):
                address = address.decode()
            return ipaddress.ip_address(address).is_loopback
        return False

    def _configure_curve(self, server):
        """Set the curve configuration of the socket depending on whether we
        are a server or not"""
        orig_server = self.curve_server
        if server:
            self.curve_publickey = self.context.server_publickey
            self.curve_secretkey = self.context.server_secretkey
            self.curve_server = True
        else:
            self.curve_server = False
            self.curve_publickey = self.context.client_publickey
            self.curve_secretkey = self.context.client_secretkey
            self.curve_serverkey = self.context.server_publickey
        return orig_server
        
    def _bind_or_connect(self, addr, bind=False, connect=False):
        assert bool(bind) != bool(connect)
        method = zmq.Socket.bind if bind else zmq.Socket.connect
        if addr.startswith('inproc://'):
            # No crypto/auth on inproc:
            result = method(self, addr)
            self.secure = True
            self.tcp = False
            return result
        elif addr.startswith('tcp://'):
            if self.context.secure:
                prev_setting = self._configure_curve(server=bind)
            try:
                result = method(self, addr)
            except:
                if self.context.secure:
                    # Roll back configuration:
                    self._configure_curve(server=prev_setting)
                raise
            self.secure = self.context.secure or self._is_internal(addr)
            self.tcp = True
            return result
        else:
            raise ValueError(addr)

    def bind(self, addr):
        """wrapper around bind, configuring security"""
        return self._bind_or_connect(addr, bind=True)

    def connect(self, addr, timeout=None):
        """Wrapper around connect, configuring security. If timeout is not None, then
        this method will block until the given timeout in seconds (or forever if
        timeout=-1) or until the connection is sucessful. When called in this blocking
        way, failed authentication will be raised as an exception if on zeromq >= 4.3,
        otherwise authentication failure is not detectable. Do not set timeout if you
        already have a monitor socket for this socket, in this case you should monitor
        for connection events yourself."""
        if timeout is not None:
            monitor = self.get_monitor_socket()
        try:
            result = self._bind_or_connect(addr, connect=True)
            if timeout is None:
                return result

            # Block until we get an authentication success or failure
            end_time = time.time() + timeout
            while timeout == -1 or time.time() < end_time:
                remaining = int(min(0, end_time - time.time()) * 1000)
                if timeout == -1:
                    remaining = -1
                events = monitor.poll(remaining)
                if events:
                    event = recv_monitor_message(monitor)['event']
                    # print(event)
                    if event in CONN_SUCCESS_EVENTS:
                        return result
                    elif event in CONN_FAIL_EVENTS:
                        msg = ("Failed to authenticate with server. Ensure both client "
                               + "and server have the same shared secret.")
                        raise AuthenticationFailure(msg)
        finally:
            if timeout is not None:
                self.disable_monitor()
                monitor.close()


    def _checkinsecure(self):
        if not (self.secure or self.allow_insecure):
            raise InsecureConnection(INSECURE_ERROR)

    def send(self, *args, **kwargs):
        self._checkinsecure()
        return zmq.Socket.send(self, *args, **kwargs)

    def recv(self, flags=0, copy=True, track=False):
        self._checkinsecure()
        msg = zmq.Socket.recv(self, flags=flags, copy=False, track=track)
        if self.tcp:
            try:
                self.peer_ip = msg.get('Peer-Address')
            except zmq.error.ZMQError:
                # Sent by a non-TCP client, even though we are a TCP socket. This is
                # possible sometimes. For example, XPUB seems to receive unsubscribe
                # messages via some internal mechanism if a subscriber disconnects.
                self.peer_ip = None
        if copy:
            return msg.bytes
        else:
            return msg


class SecureContext(zmq.Context):
    """A ZeroMQ Context with SecureContext.socket() returning a
    SecureSocket(), which can authenticate and communicate securely with all
    other SecureSocket() instances with a SecureContext sharing a
    shared_secret as returned by generate_shared_secret() and distributed
    securely to all authorised peers."""

    _socket_class = SecureSocket
    _instances = weakref.WeakValueDictionary()
    # Dummy class attrs to distinguish from zmq options:
    secure = False
    client_publickey = None
    client_secretkey = None
    server_publickey = None
    server_secretkey = None

    def __init__(self, io_threads=1, shared_secret=None):
        zmq.Context.__init__(self, io_threads)
        if shared_secret is not None:
            _check_versions()
            keys = _unpack_shared_secret(shared_secret)
            self.client_secretkey, self.server_secretkey = keys
            self.client_publickey = zmq.curve_public(self.client_secretkey)
            self.server_publickey = zmq.curve_public(self.server_secretkey)
            # Don't hold ref to auth: needed for auto cleanup at shutdown:
            auth = zmq.auth.thread.ThreadAuthenticator(self)
            auth.start()
            # Allow only clients who have the client public key:
            auth.thread.authenticator.allow_any = False
            auth.thread.authenticator.certs['*'] = {self.client_publickey: True}
            self.secure = True

    @classmethod
    def instance(cls, io_threads=1, shared_secret=None):
        """Returns a shared instance with the same shared secret, if there is one,
        otherwise creates it. If an instance already exists, io_threads will be ignored,
        otherwise it will be used in the new instance. Takes into account subclasses
        such that a subclass calling this method will always get back an instance of its
        own class"""
        try:
            return cls._instances[cls, shared_secret]
        except KeyError:
            instance = cls(io_threads, shared_secret=shared_secret)
            cls._instances[cls, shared_secret] = instance
            return instance


# if __name__ == '__main__':
#     shared_secret = generate_shared_secret()
#     other_secret = generate_shared_secret()

#     server_ctx = SecureContext.instance(shared_secret=shared_secret)
#     client_ctx = SecureContext.instance(shared_secret=shared_secret)

#     server = server_ctx.socket(zmq.REP)
#     client = client_ctx.socket(zmq.REQ)

#     # server.bind("tcp://127.0.0.1:6666")
#     client.connect("tcp://127.0.0.1:6666", timeout=-1)