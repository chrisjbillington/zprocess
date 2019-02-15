from __future__ import print_function, unicode_literals, division, absolute_import
import sys
import threading
import socket
import select
from struct import pack, unpack
from binascii import hexlify, unhexlify
from weakref import WeakSet

import ipaddress
import zmq

PY2 = sys.version_info.major == 2
if PY2:
    str = unicode
    from StringIO import StringIO as BytesIO
else:
    from io import BytesIO


BUFFER_SIZE = 8192
TIMEOUT = 1

VERSION_FIVE = 0x05

# Auth:
NO_AUTH = 0x00
AUTH_NOT_SUPPORTED = 0xFF

# Command:
TCP_CONNECT = 0x01
TCP_BIND = 0x02

# Address type:
IPV4 = 0x01
DOMAIN = 0x03
IPV6 = 0x04

# status:
GRANTED = 0x00
FAILURE = 0x01
DENIED = 0x02
NETWORK_UNREACHABLE = 0x03
HOST_UNREACHABLE = 0x04
CONNECTION_REFUSED = 0x05
TTL_EXPIRED = 0x06
COMMAND_NOT_SUPPORTED_OR_PROTOCOL_ERROR = 0x07
ADDRESS_TYPE_NOT_SUPPORTED = 0x08


class SocketClosed(RuntimeError):
    pass


class RouteNotAllowed(ValueError):
    pass


class RecvAll(object):
    """Buffer for receiving the exact number of bytes requested from a file-like
    object."""

    def __init__(self, sock, closed_callback=None):
        self.sock = sock
        self.data = b''
        self.closed_callback = closed_callback

    def __call__(self, n):
        """Receive and return exactly n bytes, unless receiving times out or the file is
        closed. Calls closed_callback if the file closes, or returns less than n bytes
        if closed_callback is None. The data received so far will be available as
        self.data if closed_callback, say, raises an exception."""
        assert isinstance(n, int)
        while len(self.data) < n:
            new_data = self.sock.recv(BUFFER_SIZE)
            if not new_data:
                if self.closed_callback is not None:
                    self.closed_callback()
                    return self.data
            self.data += new_data
        data, self.data = self.data[:n], self.data[n:]
        return data


def parse_address(readfunc):
    """Parse an address in binary SOCKS 5 format given a readfunc(n) which reliably
    returns the number of bytes requested. Returns the address_type, the address as
    a string, port number as an int, and the bytes of the binary message that was
    read. Stops reading and returns None for the address and port if the address
    type was not one of IPV4, IPV6 or DOMAIN"""
    address_message = readfunc(1)
    address_type, = unpack('B', address_message)
    if address_type == IPV4:
        raw_address = readfunc(4)
        address = ipaddress.IPv4Address(raw_address).compressed
    elif address_type == IPV6:
        raw_address = readfunc(16)
        address = ipaddress.IPv6Address(raw_address).compressed
    elif address_type == DOMAIN:
        n = readfunc(1)
        address_message += n
        n, = unpack('B', n)
        address = raw_address = readfunc(n)
    else:
        return address_type, None, None, address_message
    address_message += raw_address
    raw_port = readfunc(2)
    address_message += raw_port
    port, = unpack('!H', raw_port)
    return address_type, address, port, address_message


def str_to_hops(endpoint):
    """Take a string such as '127.0.0.1:9001|192.168.1.1:9002|[::1]:9000'
    representing a multihop SOCKS 5 proxied connection (here for example including an
    IPV6 address), and return a list of (ip, port) tuples"""
    result = []
    for hop in endpoint.split('|'):
        host, port = hop.rsplit(':', 1)
        if host.startswith('[') and host.endswith(']'):
            host = host[1:-1]
        host = ipaddress.ip_address(host).compressed
        port = int(port)
        result.append((host, port))
    return result


def hops_to_str(hops):
    """Concatenate together a list of (host, port) hops into a string such as
    '127.0.0.1:9001|192.168.1.1:9002|[::1]:9000' appropriate for printing or passing to
    a zeromq connect() call"""
    formatted_hops = []
    for host, port in hops:
        ip = ipaddress.ip_address(host)
        if ip.version == 4:
            host = ip.compressed
        elif ip.version == 6:
            host = '[%s]' % ip.compressed
        else:
            assert False
        formatted_hops.append('%s:%s' % (host, port))
    return '|'.join(formatted_hops)


def hops_to_addr(hops):
    """Take a list of (host, port) tuples as returned by endpoint_str_to_hops(), and
    return a hex encoded string of the following bytes:
        [num_hops]: 1 byte
        then for each hop:
            [addr_type]: 1 byte, IPV4 or IPV6 as defined in SOCKS 5
            [addr]: the IP address as a network-order integer, 4 bytes for IPV4 and 16
                    bytes for IPV6
            [port]: network order integer, 2 bytes

    the final hop lacks the port field, which is instead appended to the string as a
    colon followed by the port number as ascii digits, as it was passed in. In this way,
    a multihop request can be encoded as a single domain_name:port string, where
    domain_name encodes the details of the hops to made to get to the final host.

    If there is only one hop, just return host:port as a string with no further
    encoding"""
    if not hops:
        raise ValueError("no hops")
    if len(hops) == 1:
        return "%s:%d" % hops[0]
    packed_hops = pack('B', len(hops))
    for host, port in hops:
        ip = ipaddress.ip_address(host)
        if ip.version == 4:
            packed_hops += pack('B', IPV4)
        elif ip.version == 6:
            packed_hops += pack('B', IPV6)
        else:
            raise ValueError(ip.version)
        packed_hops += ip.packed
        packed_hops += pack('!H', int(port))
    return hexlify(packed_hops[:-2]).decode() + ':%d' % port


def addr_to_hops(packed):
    """Unpack a multihop request encoded as a domain:port, as returned by
    hops_to_domain(), into a list of tuples of ip addresses and ports"""
    def no_more_bytes():
        raise ValueError("Input ended unexpectedly while parsing hops")
    # Add the final port to the packed data so it can all be parsed the same way:
    packed_hops, final_port = packed.rsplit(':', 1)
    packed_hops += hexlify(pack('!H', final_port))
    read_bytes = RecvAll(BytesIO(unhexlify(packed_hops)), no_more_bytes)
    n_hops, = unpack('B', read_bytes(1))
    hops = []
    for _ in range(n_hops):
        addr_type, address, port, _ = parse_address(read_bytes) 
        if addr_type not in [IPV4, IPV6]:
            raise ValueError("Invalid addr_type %s" % addr_type)
        hops.append(address, port)
    return hops


class SocksProxyConnection(object):
    """Class representing a connection from a client to an endpoint via a socks proxy"""
    def __init__(self, client, address, socks_proxy_server):
        self.client = client
        self.source_host = ipaddress.ip_address(str(address[0]))
        self.source_port = address[1]
        self.thread = None
        self.client_recvall = RecvAll(self.client, self.end)
        self.sendall = self.client.sendall
        self.socks_proxy_server = socks_proxy_server
        self.server = None
        self.server_recvall = None
        self.ended = False

    def start(self):
        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True
        print("Starting Connection")
        self.thread.start()

    def end(self, reason='', raise_exc=True):
        """Shutdown remaining open sockets and raise SocketClosed, unless
        raise_exc=False. Reason string will be passed to the exception raised.
        """
        self.ended = True
        try:
            self.client.shutdown(socket.SHUT_RDWR)
        except (OSError, AttributeError):
            pass
        try:
            self.client.close()
        except (OSError, AttributeError):
            pass
        self.client = None
        self.client_recvall = None
        try:
            self.server.shutdown(socket.SHUT_RDWR)
        except (OSError, AttributeError):
            pass
        try:
            self.server.close()
        except (OSError, AttributeError):
            pass
        self.server = None
        self.server_recvall = None
        if raise_exc:
            raise SocketClosed(reason)

    def server_init(self):
        """Communicate using the SOCKS 5 protocol, acting as the server, to setup a
        connection on behalf of the client"""
        version, n = unpack('BB', self.client_recvall(2))
        auth_methods = unpack('B' * n, self.client_recvall(n))
        if version != VERSION_FIVE:
            self.end('Not SOCKS 5')
        if not NO_AUTH in auth_methods:
            self.client.sendall(pack('BB', VERSION_FIVE, AUTH_NOT_SUPPORTED))
            self.end('Client does not support no-auth')
        self.client.sendall(pack('BB', VERSION_FIVE, NO_AUTH))
        version, command_code = unpack('BBx', self.client_recvall(3))
        address_details = parse_address(self.client_recvall)
        address_type, address, port, address_message = address_details
        if address_type not in [IPV4, IPV6, DOMAIN]:
            self.end('client gave invalid address type')
        if version != VERSION_FIVE or command_code != TCP_CONNECT:
            # We only support TCP_CONNECT for the moment
            status = COMMAND_NOT_SUPPORTED_OR_PROTOCOL_ERROR
        else:
            status = self.connect(address_type, address, port)
        response = pack('BBx', VERSION_FIVE, status) + address_message
        self.client.sendall(response)
        if status != GRANTED:
            self.end('not granted')# TODO: why?

    def connect_bare(self, address_type, address, port):
        if not self.socks_proxy_server.allows(
            self.source_host, self.source_port, address, port
        ):
            raise RouteNotAllowed()
        if address_type == IPV4:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.connect((address, port))
        elif address_type == IPV6:
            self.server = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            addrinfo = socket.getaddrinfo(
                address, port, socket.AF_INET6, 0, socket.SOL_TCP
            )
            self.server.connect(addrinfo[0][-1])
        else:
            raise ValueError(address_type)
        self.server_recvall = RecvAll(self.server, self)
        self.server.settimeout(TIMEOUT)

    def connect(self, address_type, address, port):
        if address_type in [IPV4, IPV6]:
            try:
                self.connect_bare(address_type, address, port)
            except RouteNotAllowed:
                return DENIED
        elif address_type == DOMAIN:
            # DOMAIN address. We don't interpret domains as actual hostnames, instead we
            # treat them as a hex-encoded list of SOCKS 5 hops in the format:
            # [n_hops][addr_type][addr][port][...] where n_hops is 1 byte, and the rest
            # of the message is n_hops messages packed the same way as a single SOCKS 5
            # destination specification. The last hop lacks a port - the port from the
            # original socks request is used.
            bytes_io = BytesIO(unhexlify(address) + pack('!H', port))
            n_hops, = unpack('B', bytes_io.read(1))
            if n_hops < 1:
                # A single hop should be given with an IPV4 or IPV6 address, not our
                # DOMAIN abuse:
                return ADDRESS_TYPE_NOT_SUPPORTED
            first_hop_details = parse_address(bytes_io.read)
            proxy_address_type, proxy_addr, proxy_port, _ = first_hop_details
            try:
                self.connect_bare(proxy_address_type, proxy_addr, proxy_port)
            except RouteNotAllowed:
                return DENIED
            if n_hops == 1:
                # We're the last hop:
                return GRANTED
            else:
                # Reduce n_hops by one and send remaining hops:
                addr = hexlify(pack('B', n_hops - 1) + bytes_io.read()[:-2])
                msg = pack('BB', DOMAIN, len(addr)) + addr + pack('!H', port)
                return self.client_init(msg)
        else:
            return COMMAND_NOT_SUPPORTED_OR_PROTOCOL_ERROR

    def client_init(self, addr_message):
        """Communicate using the SOCKS 5 protocol, acting as the client, to setup a
        connection to a destination through another socks server to a destination
        specified in the byte-packed addr_message"""
        self.server.sendall(pack('BBB', VERSION_FIVE, 1, NO_AUTH))
        version, auth = unpack('BB', self.server_recvall(2))
        if version != VERSION_FIVE:
            self.end()
        if auth != NO_AUTH:
            return AUTH_NOT_SUPPORTED
        self.server.sendall(pack('BBx', VERSION_FIVE, TCP_CONNECT) + addr_message)
        version, status = unpack('BBx', self.server_recvall(3))
        echo_addr_message =  self.server_recvall(len(addr_message))
        if version != VERSION_FIVE or echo_addr_message != addr_message:
            self.end()
        return status

    def do_forwarding(self):
        while True:
            # Now forward messages between them until one closes:
            # print('selecting')
            socks = [self.server, self.client]
            readable, _, exceptional = select.select(socks, [], socks)
            # print('select returned')
            # print('exceptional:', exceptional)
            for sock in readable:
                if sock is self.server:
                    # print('sock is server')
                    other_sock = self.client
                else:
                    # print('sock is client')
                    other_sock = self.server
                try:
                    print(self.source_port, 'recving, ended =', self.ended)
                    data = sock.recv(BUFFER_SIZE)
                except OSError:
                    if self.ended:
                        print(self.source_port, 'returning')
                        return
                    raise
                if not data:
                    self.end()
                # print('sending')
                try:
                    other_sock.send(data)
                except OSError:
                    if self.ended:
                        print(self.source_port, 'returning')
                        return
                    raise


    def run(self):
        self.client.settimeout(TIMEOUT)
        try:
            try:
                print(self.source_port, 'server_init')
                self.server_init()
            except socket.timeout:
                self.end()
                return
            self.server.settimeout(None)
            self.client.settimeout(None)
            import time
            print(self.source_port, 'do_forwarding')
            self.do_forwarding()
        except SocketClosed:
            print(self.source_port, 'returning')
            return
        finally:
            self.end(raise_exc=False)
        print(self.source_port, 'returning')

    def stop(self):
        self.end(raise_exc=False)
        print("Join Connection")
        self.thread.join()
        self.thread = None

        
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
                self.source_port_range = range(source_port, source_port+1)
            else:
                lower, upper = source_port
                self.source_port_range = range(lower, upper+1)
        else:
            self.source_port_range = '*'
        if dest_port != '*':
            if isinstance(dest_port, int):
                self.source_port_range = range(dest_port, dest_port+1)
            else:
                lower, upper = dest_port
                self.dest_port_range = range(lower, upper+1)
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
    def __init__(self, port):
        self.port = port
        self.rules = set()
        self.listener = None
        self.connections = WeakSet()
        self.started = threading.Event()
        self.mainloop_thread = None
        self.shutting_down = False

    def allows(self, source_host, source_port, dest_host, dest_port):
        """Whether this route is allowed according to our current set of rules"""
        for rule in self.rules.copy():
            if rule.allows(source_host, source_port, dest_host, dest_port):
                return True
        return False

    def run(self):
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(('0.0.0.0', self.port))
        self.listener.listen(5)
        self.started.set()
        while True:
            try:
                (client, address) = self.listener.accept()
            except OSError:
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
        print("Starting server")
        self.mainloop_thread.start()
        self.started.wait()

    def shutdown(self):
        self.shutting_down = True
        self.listener.shutdown(socket.SHUT_RDWR)
        self.listener.close()
        print("Joining server")
        self.mainloop_thread.join()
        self.mainloop_thread = None
        print("server over")
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
    from zprocess.security import SecureContext, generate_shared_secret

    socks_proxy =SocksProxyServer(9001)
    socks_proxy2 = SocksProxyServer(9002)

    socks_proxy.start()
    socks_proxy2.start()

    rule_1 = AllowRule('127.0.0.1', '*', '127.0.0.1', '*')
    rule_2 = AllowRule('127.0.0.1', '*', '::1', '*')
    socks_proxy.rules.add(rule_1)
    socks_proxy.rules.add(rule_2)
    socks_proxy2.rules.add(rule_1)
    socks_proxy2.rules.add(rule_2)

    context = SecureContext(shared_secret=generate_shared_secret())
    zmq_server = context.socket(zmq.REP)
    zmq_client = context.socket(zmq.REQ)

    # zmq_client.socks_proxy = b'127.0.0.1:9001'
    zmq_client.IPV6 = 1
    zmq_server.IPV6 = 1

    zmq_server.bind('tcp://::1:9000')

    print('about to connect...')
    zmq_client.connect('tcp://127.0.0.1:9001|127.0.0.1:9002|::1:9000')

    print('about to send...')
    zmq_client.send(b'hello')

    print('about to recv...')
    print(zmq_server.recv())

    socks_proxy.shutdown()
    socks_proxy2.shutdown()

    print(threading.active_count())