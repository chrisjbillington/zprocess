import sys
import base64
import weakref
import threading
import ipaddress
from time import monotonic

import zmq
import zmq.auth.thread
from zmq.utils.monitor import recv_monitor_message

from zprocess.utils import _get_fileno
from zprocess.utils import gethostbyname, Interrupted, TimeoutError


PYZMQ_VER_MAJOR = int(zmq.__version__.split('.')[0])

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


def ip_is_loopback(ip):
    """Return whether an IP address is on the loopback interface"""
    ip = ipaddress.ip_address(ip)
    if ip.version == 6 and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    return ip.is_loopback


def generate_shared_secret():
    """Compute a new pair of random CurveZMQ secret keys, decode them from z85
    encoding, and return the result as a base64 encoded unicode string as suitable
    for passing to SecureContext() or storing on disk. We use base64 because it is
    more compatible with Python config files than z85."""
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

        # Disabled for now, have observed problems with BLACS subprocesses when using a
        # remote process server on localhost with no encryption:

        # Enable IPv6 by default:
        self.ipv6 = True

    def _is_internal(self, endpoint):
        """Return whether a bind or connect endpoint is on an internal
        interface"""
        if endpoint.startswith('inproc://'):
            return True
        if endpoint.startswith('tcp://'):
            host = endpoint.split('//', 1)[1].rsplit(':', 1)[0]
            if host.startswith('[') and host.endswith(']'):
                host = host[1:-1]
            if host == '*':
                return False
            address = gethostbyname(host)
            if isinstance(address, bytes):
                address = address.decode()
            return ip_is_loopback(address)
        return False

    def _configure_curve(self, server):
        """Set the curve configuration of the socket depending on whether we
        are a server or not"""
        orig_server = self.curve_server
        if server:
            self.curve_server = True
            self.zap_domain = self.context.zap_domain
            self.curve_publickey = self.context.server_publickey
            self.curve_secretkey = self.context.server_secretkey
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

    def connect(self, addr, timeout=None, interruptor=None):
        """Wrapper around connect, configuring security. If timeout is not None, then
        this method will block until the given timeout in ms (or forever if timeout=-1)
        or until the connection is sucessful or interrupted. When called in this
        blocking way, failed authentication will be raised as an exception. Do not set
        timeout if you already have a monitor socket for this socket, in this case you
        should monitor for connection events yourself."""

        # Not allowed to insecurely connect to external addresses unless
        # allow_insecure specified:
        if not (self.secure or self._is_internal(addr) or self.allow_insecure):
            raise InsecureConnection(INSECURE_CONNECT_ERROR % addr)

        if timeout is not None:
            monitor = self.get_monitor_socket()
            poller = zmq.Poller()
            poller.register(monitor, zmq.POLLIN)
            if interruptor is not None:
                interruption_sock = interruptor.subscribe()
                poller.register(interruption_sock)
            if timeout != -1:
                # Convert to s:
                timeout /= 1000

        try:
            result = self._bind_or_connect(addr, connect=True)
            if timeout is None:
                return result
            # Block until we get an authentication success or failure
            deadline = monotonic() + timeout
            while True:
                if timeout == -1:
                    remaining = None
                else:
                    remaining = (deadline - monotonic()) * 1000  # ms
                events = dict(poller.poll(remaining))
                if not events:
                    raise TimeoutError('Could not connect to server: timed out')
                if interruptor is not None and interruption_sock in events:
                    raise Interrupted(interruption_sock.recv().decode('utf8'))
                assert events[monitor] == zmq.POLLIN
                event = recv_monitor_message(monitor)['event']
                if event in CONN_SUCCESS_EVENTS:
                    return result
                elif event in CONN_FAIL_EVENTS:
                    msg = ("Failed to authenticate with server. Ensure both client "
                           + "and server have the same shared secret.")
                    raise AuthenticationFailure(msg)
        finally:
            if timeout is not None:
                if interruptor is not None:
                    poller.unregister(interruption_sock)
                    interruptor.unsubscribe()
                poller.unregister(monitor)
                self.disable_monitor()
                monitor.close()

    def send(self, *args, **kwargs):
        return zmq.Socket.send(self, *args, **kwargs)

    def recv(self, flags=0, copy=True, track=False):
        while True:
            msg = zmq.Socket.recv(self, flags=flags, copy=False, track=track)
            if self.tcp:
                try:
                    peer_ip = msg.get('Peer-Address')
                    peer_ip = ipaddress.ip_address(peer_ip)
                    if peer_ip.version == 6 and peer_ip.ipv4_mapped is not None:
                        peer_ip = peer_ip.ipv4_mapped
                    self.peer_ip = str(peer_ip)
                except zmq.error.ZMQError:
                    # Sent by a non-TCP client, even though we are a TCP socket. This is
                    # possible sometimes. For example, XPUB seems to receive unsubscribe
                    # messages via some internal mechanism if a subscriber disconnects.
                    self.peer_ip = None
                else:
                    is_loopback = ip_is_loopback(self.peer_ip)
                    if not (is_loopback or self.secure or self.allow_insecure):
                        # Unsecured data on external interface and allow_insecure has
                        # not been set. Print a message and ignore it.
                        msg = INSECURE_RECV_WARNING % self.peer_ip
                        if self.logger is not None:
                            self.logger.warning(msg)
                        elif sys.stderr is not None and _get_fileno(sys.stderr) >= 0:
                            sys.stderr.write(msg + '\n')
                        # Disregard the insecure message and call recv() again
                        continue
            if copy:
                return msg.bytes
            else:
                return msg


class _ThreadAuthenticator:
    # We roll our own thread authenticator (just implementing what we need) since a)
    # zmq.auth.thread.ThreadAuthenticator uses asyncio, and we do not want to express an
    # opinion on the kerfuffle that is Windows asyncio selectors (see:
    # https://github.com/zeromq/pyzmq/issues/1423) and impose it on the rest of the
    # interpreter, when we're not even using async stuff ourself and b)
    # zmq.auth.thread.ThreadAuthenticator spawns a non-daemon thread which is difficult
    # to ensure gets shut down at interpreter shutdown, and otherwise holds the
    # interpreter open.
    def __init__(self, ctx, zap_domain, allowed_clients):
        if PYZMQ_VER_MAJOR >= 25:
            self.zap_socket = ctx.socket(zmq.REP, socket_class=zmq.Socket)
        else:
            self.zap_socket = ctx.socket(zmq.REP)
        self.zap_socket.linger = 1
        self.zap_socket.bind("inproc://zeromq.zap.01")

        # Note: we hold a reference to the zap socket, since if the thread crashes, we
        # don't want the socket to be cleaned up and closed - that would have zmq
        # (stupidly and dangerously) fall back to its default authentication method,
        # which is to allow all.
        self.thread = threading.Thread(
            target=self.run,
            args=(self.zap_socket, zap_domain, allowed_clients),
            daemon=True,
        )
        self.started = threading.Event()
        self.thread.start()

    def run(self, zap_socket, zap_domain, allowed_clients):
        VERSION = b'1.0'
        MECHANISM = b'CURVE'
        while True:
            try:
                msg = zap_socket.recv_multipart()
            except zmq.error.ContextTerminated:
                zap_socket.close()
                return
            version, request_id, domain, address, identity, mechanism = msg[:6]
            credentials = msg[6:]
            if version != VERSION:
                status_code = b"400"
                status_text = b"Invalid version"
                user_id = b""
            elif mechanism != MECHANISM:
                status_code = b"400"
                status_text = b"Security mechanism not supported"
                user_id = b""
            elif domain != zap_domain:
                status_code = b"400"
                status_text = b"Unknown domain"
                user_id = b""
            else:
                key = zmq.utils.z85.encode(credentials[0])
                if key in allowed_clients:
                    status_code = b"200"
                    status_text = b"OK"
                    user_id = key
                else:
                    status_code = b"400"
                    status_text = b"Unknown key"
                    user_id = b""
            response = [VERSION, request_id, status_code, status_text, user_id, b""]
            zap_socket.send_multipart(response)


class SecureContext(zmq.Context):
    """A ZeroMQ Context with SecureContext.socket() returning a
    SecureSocket(), which can authenticate and communicate securely with all
    other SecureSocket() instances with a SecureContext sharing a
    shared_secret as returned by generate_shared_secret() and distributed
    securely to all authorised peers."""

    _socket_class = SecureSocket
    _instances = weakref.WeakValueDictionary()
    zap_domain = b"zprocess"

    # Dummy class attrs to distinguish from zmq options:
    auth = None
    secure = False
    client_publickey = None
    client_secretkey = None
    server_publickey = None
    server_secretkey = None

    def __init__(self, io_threads=1, shared_secret=None):
        zmq.Context.__init__(self, io_threads)
        if shared_secret is not None:
            keys = _unpack_shared_secret(shared_secret)
            self.client_secretkey, self.server_secretkey = keys
            self.client_publickey = zmq.curve_public(self.client_secretkey)
            self.server_publickey = zmq.curve_public(self.server_secretkey)
            
            # Note: it is crucial we hold a reference to the authenticator, so that in
            # the case the zap authentication thread crashes, the zap socket does not
            # get cleaned up and closed - zmq will interpret that situation as us not
            # requiring any authentication.
            self.auth = _ThreadAuthenticator(
                self,
                zap_domain=self.zap_domain,
                allowed_clients=[self.client_publickey],
            )
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
