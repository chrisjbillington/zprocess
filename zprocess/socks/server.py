from __future__ import print_function, unicode_literals, division, absolute_import
import sys
import os
import threading
import socket
from select import select
from struct import pack, unpack
from weakref import WeakSet
import errno

import ipaddress
import zmq

PY2 = sys.version_info.major == 2
if PY2:
    str = unicode
    from StringIO import StringIO as BytesIO
else:
    from io import BytesIO

from zprocess.socks import (
    socks5,
    parse_address,
    pack_address,
    domain_addr_to_hops,
    hops_to_str,
    DEFAULT_PORT,
)
from zprocess.utils import setup_logging

BUFFER_SIZE = 8192
TIMEOUT = 5

# Mappings to and from OS error numbers
ERRNO_TO_STATUS = {
    errno.ECONNREFUSED: socks5.CONNECTION_REFUSED,
    errno.ENETUNREACH: socks5.NETWORK_UNREACHABLE,
    errno.EHOSTUNREACH: socks5.HOST_UNREACHABLE,
}

STATUS_TO_STR = {value: os.strerror(key) for key, value in ERRNO_TO_STATUS.items()}
STATUS_TO_STR.update(
    {
        socks5.DENIED: "denied: not allowed by any rule",
        socks5.TTL_EXPIRED: "connecting to destination timed out",
    }
)

def ordinal(n):
    """Return the ordinal for an int, eg 1st, 2nd, 3rd. From user Gareth on
    codegolf.stackexchange.com """
    suffixes = {1: "st", 2: "nd", 3: "rd"}
    return str(n) + suffixes.get(n % 10 * (n % 100 not in [11, 12, 13]), "th")

class ConnectionEnded(RuntimeError):
    pass


class RouteNotAllowed(ValueError):
    pass


class RecvAll(object):
    """Buffer for receiving the exact number of bytes requested from a socket or
    file-like object, and catching errors if the socket times out, closes or if we're
    asked to shutdown via a message to (or closure of) the self_pipe. Use of self_pipe
    only valid on Windows if both it and the file-like object being read are sockets."""

    def __init__(
        self,
        sock,
        closed_callback=None,
        timeout=None,
        timeout_callback=None,
        self_pipe=None,
        self_pipe_callback=None,
    ):
        self.sock = sock
        self.data = b''
        self.closed_callback = closed_callback
        self.timeout = timeout
        self.timeout_callback = timeout_callback
        self.self_pipe = self_pipe
        self.self_pipe_callback = self_pipe_callback

    def __call__(self, n, timeout=None):
        """Receive and return exactly n bytes, unless receiving times out or the file is
        closed, or interrupted via self-pipe. Calls closed_callback, if any, if the file
        closes, then returns less than n bytes. Calls self_pipe_callback, if any, if
        interrupted, then returns potentially less than n bytes. Calls timout_callback,
        if any, if the receiving times out, then returns potentially less than n bytes.
        If callbacks raise an exception, the data received so far will be available as
        self.data. The timeout is a per-read timeout, not a total one."""
        assert isinstance(n, int)
        if timeout is None:
            timeout = self.timeout
        if self.self_pipe is not None:
            fds = [self.sock, self.self_pipe]
        else:
            fds = [self.sock]
        while len(self.data) < n:
            if timeout is not None or self.self_pipe is not None:
                ready, _, _ = select(fds, [], [], timeout)
            else:
                ready = fds
            if self.sock in ready:
                new_data = self.sock.recv(BUFFER_SIZE)
                if not new_data:
                    if self.closed_callback is not None:
                        self.closed_callback()
                    return self.data
                self.data += new_data
            if self.self_pipe in ready:
                # We've been interrupted via the self-pipe.
                if self.self_pipe_callback is not None:
                    self.self_pipe_callback()
                return self.data
            if not ready:
                # We've timed out:
                if self.timeout_callback is not None:
                    self.timeout_callback()
                    return self.data
        data, self.data = self.data[:n], self.data[n:]
        return data


class SocksProxyConnection(object):
    """Class representing a connection from a client to an endpoint via a socks proxy"""

    TIMEOUT = 5

    def __init__(self, client, address, socks_proxy_server):
        self.client = client
        self.source_host = ipaddress.ip_address(str(address[0]))
        self.source_port = address[1]
        self.thread = None
        self.client_recvall = None
        self.socks_proxy_server = socks_proxy_server
        self.logger = socks_proxy_server.logger
        self.server = None
        self.server_recvall = None
        self.self_pipe_reader = None
        self.self_pipe_writer = None
        self.established = False
        self.conn_str = None

    def start(self):
        if os.name == 'nt':
            # On Windows the self-pipe trick requires an unbound UDP socket which is
            # closed to interrupt a select() call.
            self.self_pipe_writer = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.self_pipe_reader = self.self_pipe_reader
        else:
            # On unix some other file descriptor is required for the self-pipe trick, so
            # we use a pipe
            self.self_pipe_reader, self.self_pipe_writer = os.pipe()
        self.client_recvall = RecvAll(
            self.client,
            closed_callback=lambda: self.end("closed by origin"),
            timeout=TIMEOUT,
            timeout_callback=lambda: self.end("origin timed out"),
            self_pipe=self.self_pipe_reader,
            self_pipe_callback=lambda: self.end("interrupted"),
        )
        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        # Close the write end of the self-pipe to interrupt select() calls and signify
        # that we are shutting down:
        if os.name == 'nt':
            self.self_pipe_writer.close()
        else:
            os.close(self.self_pipe_writer)
        self.thread.join()
        self.thread = None

    def run(self):
        try:
            self.server_init()
            self.established = True
            self.logger.info(
                'established: %s:%d -> %s',
                self.source_host,
                self.source_port,
                self.conn_str,
            )
            self.do_forwarding()
        except ConnectionEnded as e:
            if self.established:
                event = 'closed'
            else:
                event = 'failed'
            self.logger.info(
                '%s (%s): %s:%d -> %s',
                event,
                str(e),
                self.source_host,
                self.source_port,
                self.conn_str or '?',
            )

    def end(self, reason=''):
        """Shutdown remaining open sockets and raise CnnectionEnded(reason)"""
        self.client.close()
        self.client = None
        self.client_recvall = None
        if self.server is not None:
            self.server.close()
        self.server = None
        self.server_recvall = None
        raise ConnectionEnded(reason)

    def server_init(self):
        """Communicate using the SOCKS 5 protocol, acting as the server, to setup a
        connection on behalf of the client"""
        version, n = unpack('BB', self.client_recvall(2))
        auth_methods = unpack('B' * n, self.client_recvall(n))
        if version != socks5.VERSION:
            self.end('protocol error: not socks 5')
        if not socks5.NO_AUTH in auth_methods:
            self.client.sendall(pack('BB', socks5.VERSION, socks5.AUTH_NOT_SUPPORTED))
            self.end('authentication unsupported')
        self.client.sendall(pack('BB', socks5.VERSION, socks5.NO_AUTH))
        version, command_code = unpack('BBx', self.client_recvall(3))
        address_details = parse_address(self.client_recvall)
        address_type, address, port, address_message = address_details
        if address_type == socks5.DOMAIN:
            self.conn_str = hops_to_str(domain_addr_to_hops(address, port))
        else:
            self.conn_str = '%s:%d' % (address, port)
        if address_type not in [socks5.IPV4, socks5.IPV6, socks5.DOMAIN]:
            self.end('invalid address type')
        if version != socks5.VERSION or command_code != socks5.TCP_CONNECT:
            # We only support TCP_CONNECT for the moment
            status = socks5.COMMAND_NOT_SUPPORTED_OR_PROTOCOL_ERROR
        else:
            status, failed_hop_num = self.connect(address_type, address, port)
        response = pack('BBx', socks5.VERSION, status) + address_message
        self.client.sendall(response)
        if status != socks5.GRANTED:
            errmsg = STATUS_TO_STR[status]
            if failed_hop_num is not None:
                errmsg += " on %s hop" % ordinal(failed_hop_num)
            self.end(errmsg)

    def connect(self, address_type, address, port):
        if address_type in [socks5.IPV4, socks5.IPV6]:
            return self.connect_bare(address, port), None
        elif address_type == socks5.DOMAIN:
            # DOMAIN address. Interpret as packed multi-hop request:
            hops = domain_addr_to_hops(address, port)
            if len(hops) < 2:
                # Not multihop, should have been given as an IPV4 or IPV6 request:
                return socks5.ADDRESS_TYPE_NOT_SUPPORTED, None
            # Connect to the next socks proxy server:
            first_hop_addr, first_hop_port = hops[0]
            status = self.connect_bare(first_hop_addr, first_hop_port)
            if status != socks5.GRANTED:
                return status, 1
            # For each hop, request a connection to the next hop:
            for i, (addr, port) in enumerate(hops[1:]):
                status = self.client_init(addr, port)
                if status != socks5.GRANTED:
                    return status, i+2
            return socks5.GRANTED, None
        else:
            return socks5.COMMAND_NOT_SUPPORTED_OR_PROTOCOL_ERROR, None

    def connect_bare(self, address, port):
        try:
            if not self.socks_proxy_server.allows(
                self.source_host, self.source_port, address, port
            ):
                return socks5.DENIED
            ip_version = ipaddress.ip_address(address).version
            if ip_version == 4:
                self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                addrinfo = (address, port)
            elif ip_version == 6:
                self.server = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                addrinfo = socket.getaddrinfo(
                    address, port, socket.AF_INET6, 0, socket.SOL_TCP
                )[0][-1]
            else:
                assert False
            # Non-blocking just during connect().
            self.server.setblocking(0)
            try:
                self.server.connect(addrinfo)
            except (OSError, IOError) as e:
                if e.errno != errno.EINPROGRESS:
                    raise
            self.server.setblocking(1)
            interrupted, ready, _ = select(
                [self.self_pipe_reader], [self.server], [], TIMEOUT
            )
            if interrupted:
                self.end("interrupted")
            if not ready:
                return socks5.TTL_EXPIRED
            # Write empty message to raise connection exceptions, if any:
            self.server.send(b'')
        except (OSError, IOError) as e:
            return ERRNO_TO_STATUS[e.errno]

        self.server_recvall = RecvAll(
            self.server,
            closed_callback=lambda: self.end("closed by destination"),
            timeout=TIMEOUT,
            timeout_callback=lambda: self.end("destination timed out"),
            self_pipe=self.self_pipe_reader,
            self_pipe_callback=lambda: self.end("interrupted"),
        )
        return socks5.GRANTED

    def client_init(self, host, port):
        """Communicate using the SOCKS 5 protocol, acting as the client, to setup a
        connection to a destination through another socks server to a destination
        specified in the byte-packed addr_message"""
        self.server.sendall(pack('BBB', socks5.VERSION, 1, socks5.NO_AUTH))
        version, auth = unpack('BB', self.server_recvall(2))
        if version != socks5.VERSION:
            self.end('protocol error: not socks 5')
        if auth != socks5.NO_AUTH:
            return socks5.AUTH_NOT_SUPPORTED
        addr_message = pack_address(host, port)
        self.server.sendall(
            pack('BBx', socks5.VERSION, socks5.TCP_CONNECT) + addr_message
        )
        version, status = unpack('BBx', self.server_recvall(3))
        echo_addr_message = self.server_recvall(len(addr_message))
        if version != socks5.VERSION or echo_addr_message != addr_message:
            self.end('protocol error: not socks 5')
        return status

    def do_forwarding(self):
        while True:
            # Now forward messages between them until one closes:
            readable, _, _ = select(
                [self.server, self.client, self.self_pipe_reader], [], []
            )
            for fd in readable:
                if fd is self.self_pipe_reader:
                    # We've been interrupted via self-pipe:
                    self.end("interrupted")
                if fd is self.server:
                    other_sock = self.client
                else:
                    other_sock = self.server
                data = fd.recv(BUFFER_SIZE)
                if data:
                    other_sock.sendall(data)
                else:
                    # One of the sockets closed from the other end:
                    if fd is self.server:
                        self.end("closed by destination")
                    else:
                        self.end("closed by origin")


class AllowRule(object):
    """Class to represent allowed route. Hosts must be single IP addresses, or IP
    address ranges as strings, such as '192.168.0.0/28' - i.e. anything that can be
    passed to ipaddress.ip_network() - or "*"" to mean any IP address, and ports must be
    integers, tuples of lower and upper inclusive bounds, or "*" to mean any port"""

    def __init__(self, source_host, source_port, dest_host, dest_port):
        self.source_host = source_host
        self.dest_host = dest_host
        if self.source_host != '*':
            self.source_host = ipaddress.ip_network(source_host)
        if self.dest_host != '*':
            self.dest_host = ipaddress.ip_network(dest_host)
        if source_port != '*':
            if isinstance(source_port, int):
                self.source_port_range = range(source_port, source_port + 1)
            else:
                lower, upper = source_port
                self.source_port_range = range(lower, upper + 1)
        else:
            self.source_port_range = '*'
        if dest_port != '*':
            if isinstance(dest_port, int):
                self.source_port_range = range(dest_port, dest_port + 1)
            else:
                lower, upper = dest_port
                self.dest_port_range = range(lower, upper + 1)
        else:
            self.dest_port_range = '*'

    def allows(self, source_host, source_port, dest_host, dest_port):
        """Return whether the rule allows the given route. hosts should be srings or
        ipaddress.IPv4Address or IPv6Address objects, and ports should be integers"""
        if not isinstance(source_host, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
            source_host = ipaddress.ip_address(source_host)
        if not isinstance(dest_host, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
            dest_host = ipaddress.ip_address(dest_host)
        if self.source_host != '*' and source_host not in self.source_host:
            return False
        if self.dest_host != '*' and dest_host not in self.dest_host:
            return False
        if self.source_port_range != '*' and source_port not in self.source_port_range:
            return False
        if self.dest_port_range != '*' and dest_port not in self.dest_port_range:
            return False
        return True


class SocksProxyServer(object):
    def __init__(self, port=DEFAULT_PORT, bind_address='0.0.0.0', silent=False):
        self.port = port
        self.bind_address = bind_address
        self.silent=silent
        self.rules = set()
        self.listener = None
        self.connections = WeakSet()
        self.started = threading.Event()
        self.mainloop_thread = None
        self.shutting_down = False
        self.logger = None

    def allows(self, source_host, source_port, dest_host, dest_port):
        """Whether this route is allowed according to our current set of rules"""
        for rule in self.rules.copy():
            if rule.allows(source_host, source_port, dest_host, dest_port):
                return True
        return False

    def run(self):
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind((self.bind_address, self.port))
        self.listener.listen(5)
        self.started.set()
        while True:
            try:
                (client, address) = self.listener.accept()
            except (OSError, socket.error):
                if self.shutting_down:
                    break
                raise
            connection = SocksProxyConnection(client, address, self)
            self.connections.add(connection)
            connection.start()

    def start(self):
        """Call self.run() in a thread"""
        self.mainloop_thread = threading.Thread(target=self.run)
        self.mainloop_thread.daemon = True
        self.mainloop_thread.start()
        self.started.wait()
        self.logger = setup_logging('zprocess-socks', self.silent)
        msg = 'This is zprocess-socks server, running on %s:%d'
        self.logger.info(msg, self.bind_address, self.port)

    def shutdown(self):
        self.logger.info('Shutting down')
        self.shutting_down = True
        self.listener.shutdown(socket.SHUT_RDWR)
        self.listener.close()
        self.mainloop_thread.join()
        self.mainloop_thread = None
        while True:
            try:
                connection = self.connections.pop()
            except KeyError:
                break
            connection.stop()
        self.started.clear()
        self.listener = None
        self.shutting_down = False


if __name__ == '__main__':
    # Testing. Pipe a secure zmq connection through three hops and say hello.
    from zprocess.security import SecureContext, generate_shared_secret

    socks_proxy1 = SocksProxyServer(9001)
    socks_proxy2 = SocksProxyServer(9002)
    socks_proxy3 = SocksProxyServer(9003)

    socks_proxy1.start()
    socks_proxy2.start()
    socks_proxy3.start()

    rule_1 = AllowRule('127.0.0.1', '*', '127.0.0.1', '*')
    rule_2 = AllowRule('127.0.0.1', '*', '::1', '*')
    for proxy in [socks_proxy1, socks_proxy2, socks_proxy3]:
        proxy.rules.add(rule_1)
        proxy.rules.add(rule_2)

    context = SecureContext(shared_secret=generate_shared_secret())
    zmq_server = context.socket(zmq.REP)
    zmq_client = context.socket(zmq.REQ)

    zmq_client.IPV6 = 1
    zmq_server.IPV6 = 1

    zmq_server.bind('tcp://::1:9000')

    print('about to connect...')
    zmq_client.connect('tcp://127.0.0.1:9001|127.0.0.1:9002|127.0.0.1:9003|[::1]:9000')

    print('about to send...')
    zmq_client.send(b'hello')

    print('about to recv...')
    print(zmq_server.recv())

    print('********closing server 2********')
    socks_proxy2.shutdown()

    print('********closing server 1********')
    socks_proxy1.shutdown()

    print('********closing server 3********')
    socks_proxy3.shutdown()

    # Should only be one thread left after everything is shut down:
    print("num threads:", threading.active_count())
