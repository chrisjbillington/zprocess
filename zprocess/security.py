from __future__ import print_function, unicode_literals, division
import sys
import ctypes
import base64
import weakref
import time
from socket import gethostbyname
import os
import ipaddress
import zmq
import zmq.auth.thread
from zmq.utils.monitor import recv_monitor_message


if sys.version_info[0] == 2:
    str = unicode


_bundle_warning = """zprocess warning: pyzmq is using bundled libzmq, which on Windows
is not built with the cryptography library libsodium. Encryption/decryption will be
slow. Use the conda pyzmq package for fast cryptography.\n"""


def _check_versions():
    """Check for bundled zmq on windows. It has slow crypto - warn the user"""
    if os.name != 'nt':
        return
    try:
        import zmq.libzmq
    except ImportError:
        # No bundled zmq.
        return
    if sys.stderr is not None and sys.stderr.fileno() >= 0:
        sys.stderr.write(_bundle_warning)

# Events to tell when a conneciton has succeeded, including authentication:
CONN_SUCCESS_EVENTS = {zmq.EVENT_HANDSHAKE_SUCCEEDED}
CONN_FAIL_EVENTS = {
    zmq.EVENT_HANDSHAKE_FAILED_NO_DETAIL,
    zmq.EVENT_HANDSHAKE_FAILED_PROTOCOL,
    zmq.EVENT_HANDSHAKE_FAILED_AUTH,
}


class InsecureConnection(RuntimeError):
    pass


class AuthenticationFailure(RuntimeError):
    pass


INSECURE_CONNECT_ERROR = ' '.join(
    """Plaintext socket connecting to external address %s  not allowed. Use a shared
secret to securely communicate across the network, or set allow_insecure=True if you are
on a trusted network and you know what you are doing. Insecure communication with Python
objects can allow remote code execution and so is not safe on an untrusted network even
if messages contain no sensitive information.""".splitlines()
)


INSECURE_RECV_WARNING = ' '.join(
    """Warning: insecure message received on external interface from %s discarded. Use a
shared secret to securely communicate across the network, or set allow_insecure=True if
you are on a trusted network and you know what you are doing. Insecure communication
with Python objects can allow remote code execution and so is not safe on an untrusted
network even if messages contain no sensitive information.""".splitlines()
)


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
    InsecureConnection will be raised if connecting to an external network address, and
    incoming messages received from external addresses will be discarded. This can be
    suppressed by passing allow_insecure=True to the Context.socket() call. inproc://
    connections are not secured. For TCP connections, the IP address of the sender is
    saved as self.peer_ip after each message is received."""

    # Dummy class attrs to distinguish from zmq options:
    secure = None
    allow_insecure = None
    peer_ip = None
    tcp = None
    logger = None

    def __init__(self, *args, **kwargs):
        self.allow_insecure = kwargs.pop('allow_insecure', False)
        zmq.Socket.__init__(self, *args, **kwargs)
        # The IP address, if any, of the peer we last received a message from:
        self.peer_ip = None
        # Whether the socket is using tcp transport:
        self.tcp = False
        # A logger used for security warnings. If not set they will be written to stderr
        # instead
        self.logger = None
        self.secure = self.context.secure

    def _is_internal(self, endpoint):
        """Return whether a bind or connect endpoint is on an internal
        interface"""
        if endpoint.startswith('inproc://'):
            return True
        if endpoint.startswith('tcp://'):
            host = ''.join(''.join(endpoint.split('//')[1:]).split(':')[0])
            if host == '*':
                return False
            address = gethostbyname(host)
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
        # Use a socks proxy if the addr is in the format:
        # "tcp://socks_proxy1:port|socks_proxy2:port|destination_host:port".
        if '|' in addr:
            from zprocess.socks import str_to_hops, hops_to_domain_addr
            hops = str_to_hops(addr.split('tcp://', 1)[1])
            socks_host, socks_port = hops[0]
            host, port = hops_to_domain_addr(hops[1:])
            socks_host = gethostbyname(socks_host)
            socks_addr = '%s:%d' % (socks_host, socks_port)
            addr = 'tcp://%s:%d' % (host, port)
            self.socks_proxy = socks_addr.encode('utf8')
        assert bool(bind) != bool(connect)
        method = zmq.Socket.bind if bind else zmq.Socket.connect
        if addr.startswith('inproc://'):
            # No crypto/auth on inproc:
            result = method(self, addr)
            self.tcp = False
            return result
        elif addr.startswith('tcp://'):
            if self.secure:
                prev_setting = self._configure_curve(server=bind)
            try:
                result = method(self, addr)
            except:
                if self.secure:
                    # Roll back configuration:
                    self._configure_curve(server=prev_setting)
                raise
            self.tcp = True
            return result
        else:
            raise ValueError(addr)

    def bind(self, addr):
        """wrapper around bind, configuring security"""
        return self._bind_or_connect(addr, bind=True)

    def connect(self, addr, timeout=None):
        """Wrapper around connect, configuring security. If timeout is not None, then
        this method will block until the given timeout in ms (or forever if
        timeout=-1) or until the connection is sucessful. When called in this blocking
        way, failed authentication will be raised as an exception if on zeromq >= 4.3,
        otherwise authentication failure is not detectable. Do not set timeout if you
        already have a monitor socket for this socket, in this case you should monitor
        for connection events yourself."""
        if timeout is not None:
            monitor = self.get_monitor_socket()
            if timeout != -1:
                # Convert to s:
                timeout /= 1000
        try:
            # Not allowed to insecurely connect to external addresses unless
            # allow_insecure specified:
            if not (self.secure or self._is_internal(addr) or self.allow_insecure):
                raise InsecureConnection(INSECURE_CONNECT_ERROR % addr)
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

    def send(self, *args, **kwargs):
        return zmq.Socket.send(self, *args, **kwargs)

    def recv(self, flags=0, copy=True, track=False):
        while True:
            msg = zmq.Socket.recv(self, flags=flags, copy=False, track=track)
            if self.tcp:
                try:
                    self.peer_ip = msg.get('Peer-Address')
                except zmq.error.ZMQError:
                    # Sent by a non-TCP client, even though we are a TCP socket. This is
                    # possible sometimes. For example, XPUB seems to receive unsubscribe
                    # messages via some internal mechanism if a subscriber disconnects.
                    self.peer_ip = None
                else:
                    is_loopback = ipaddress.ip_address(self.peer_ip).is_loopback
                    if not (is_loopback or self.secure or self.allow_insecure):
                        # Unsecured data on external interface and allow_insecure has
                        # not been set. Print a message and ignore it.
                        msg = INSECURE_RECV_WARNING % self.peer_ip
                        if self.logger is not None:
                            self.logger.warning(msg)
                        elif sys.stderr is not None and sys.stderr.fileno() >= 0:
                            sys.stderr.write(msg + '\n')
                        # Disregard the insecure message and call recv() again
                        continue
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

#     server.bind("tcp://127.0.0.1:6666")
#     client.connect("tcp://127.0.0.1:6666", timeout=-1)
#     client.send(b'hello')
#     assert server.recv() == b'hello'
