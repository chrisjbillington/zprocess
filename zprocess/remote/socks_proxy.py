from __future__ import print_function, unicode_literals, division, absolute_import
import sys
import time
import threading
import socket
import select
from struct import pack, unpack

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


class RecvAll(object):
    """Buffer for receiving the exact number of bytes requested from a socket."""

    def __init__(self, sock, socks_proxy_connection):
        self.sock = sock
        self.data = b''
        self.socks_proxy_connection = socks_proxy_connection

    def __call__(self, n):
        """Receive and return exactly n bytes, unless receiving times out or the socket
        is closed. Closes the socket on our end and raises SocketClosed if the sender
        closes the socket. The data received so far will be available as self.data"""
        assert isinstance(n, int)
        while len(self.data) < n:
            new_data = self.sock.recv(BUFFER_SIZE)
            if not new_data:
                self.socks_proxy_connection.end()
            self.data += new_data
        data, self.data = self.data[:n], self.data[n:]
        print('client sent:', data)
        return data


class SocksProxyConnection(object):
    """Class representing a connection from a client to an endpoint via a socks proxy"""
    def __init__(self, client, address, socks_proxy_server):
        self.client = client
        self.address = address
        self.thread = None
        self.client_recvall = RecvAll(self.client, self)
        self.sendall = self.client.sendall
        self.socks_proxy_server = socks_proxy_server
        self.server = None
        self.server_recvall = None

    def start(self):
        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True
        self.thread.start()

    def parse_address(self, readfunc):
        """Parse an address in binary SOCKS 5 format given a readfunc(n) which reliably
        returns the number of bytes requested. Returns the address_type, the address as
        a string, port number as an int, and the bytes of the binary message that was
        read. Stops reading and returns None for the address and port if the address
        type was not one of IPV4, IPV6 or DOMAIN"""
        address_message = readfunc(1)
        address_type, = unpack('B', address_message)
        if address_type == IPV4:
            print('address is IPV4')
            raw_address = readfunc(4)
            address = ipaddress.IPv4Address(raw_address).exploded
        elif address_type == IPV6:
            print('address is IPV6')
            raw_address = readfunc(16)
            address = ipaddress.IPv6Address(raw_address).exploded
        elif address_type == DOMAIN:
            n = readfunc(1)
            address_message += n
            n, = unpack('B', n)
            address = raw_address = self.client_recvall(n)
        else:
            return address_type, None, None, address_message
        address_message += raw_address
        raw_port = self.client_recvall(2)
        address_message += raw_port
        port, = unpack('!H', raw_port)
        print('address is', address)
        print('port is', port)
        return address_type, address, port, address_message

    def end(self):
        print('end')
        """Shutdown the socket and raise SocketClosed"""
        try:
            self.client.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.client.close()
        self.client = None
        self.client_recvall = None
        if self.server is not None:
            self.server.shutdown(socket.SHUT_RDWR)
            self.server.close()
        self.server = None
        self.server.recvall = None
        raise SocketClosed()

    def server_init(self):
        """Communicate using the SOCKS 5 protocol, acting as the server, to setup a
        connection on behalf of the client"""
        print('init')
        print('getting version, n_auth_methods')
        version, n = unpack('BB', self.client_recvall(2))
        print('getting auth_methods')
        auth_methods = unpack('B' * n, self.client_recvall(n))
        if version != VERSION_FIVE:
            print('wrong version')
            self.end()
        if not NO_AUTH in auth_methods:
            self.client.sendall(pack('BB', VERSION_FIVE, AUTH_NOT_SUPPORTED))
            print('auth not supported')
            self.end()
        print('sending chosen auth method')
        self.client.sendall(pack('BB', VERSION_FIVE, NO_AUTH))
        print('recving command code')
        version, command_code = unpack('BBx', self.client_recvall(3))
        address_details = self.parse_address(self.client_recvall)
        address_type, address, port, address_message = address_details
        if address_type not in [IPV4, IPV6, DOMAIN]:
            print('address type not valid')
            self.end()
        # TODO check with self.socks_proxy_server if address allowed
        if version != VERSION_FIVE or command_code != TCP_CONNECT:
            # We only support TCP_CONNECT for the moment
            status = COMMAND_NOT_SUPPORTED_OR_PROTOCOL_ERROR
        else:
            status = self.connect(address_type, address, port)
        response = pack('BBx', VERSION_FIVE, status) + address_message
        self.client.sendall(response)
        if status != GRANTED:
            print('not granted')
            self.end()

    def connect(self, address_type, address, port):
        print('connect')
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
            # DOMAIN address. We don't interpret domains as actual hostnames, instead we
            # treat them as a byte-packed list of SOCKS 5 hops in the format:
            # [n_hops][addr_type][addr][port][...] where n_hops is 1 byte, and the rest
            # of the message is n_hops messages packed the same way as a single SOCKS 5
            # destination specification.
            bytes_io = BytesIO(address)
            n_hops = unpack(bytes_io.read(1))
            first_hop_details = self.parse_address(bytes_io.read)
            proxy_address_type, proxy_addr, proxy_port, _ = first_hop_details
            first_hop_status = self.connect(proxy_address_type, proxy_addr, proxy_port)
            if first_hop_status != GRANTED:
                return first_hop_status
            n_hops -= 1
            while n_hops > 0:
                _, _, _, addr_message = self.parse_address(bytes_io.read)
                status = self.client_init(addr_message)
                if status != GRANTED:
                    return status
        self.server_recvall = RecvAll(self.server, self)
        self.server.settimeout(TIMEOUT)
        return GRANTED

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
        print('do_forwarding')
        socks = {self.client: 'client', self.server: 'server'}
        while True:
            # Now forward messages between them until one closes:
            readable, _, _ = select.select([self.server, self.client], [], [])
            for sock in readable:
                if sock is self.server:
                    other_sock = self.client
                else:
                    other_sock = self.server
                data = sock.recv(BUFFER_SIZE)
                print(socks[sock], ':', data)
                if not data:
                    self.end()
                other_sock.send(data)

    def run(self):
        self.client.settimeout(TIMEOUT)
        try:
            try:
                self.server_init()
            except socket.timeout:
                self.end()
                return
            self.server.settimeout(None)
            self.client.settimeout(None)
            self.do_forwarding()
        except SocketClosed:
            return

        
class SocksProxyServer(object):
    def __init__(self, port):
        self.port = port
        self.allowed_routes = set()
        self.listener = None

    def run(self):
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(('0.0.0.0', self.port))
        self.listener.listen(5)
        while True:
            (client, address) = self.listener.accept()
            print('got a client')
            SocksProxyConnection(client, address, self).start()

        

from zprocess.security import SecureContext, generate_shared_secret

socks_proxy = threading.Thread(target=SocksProxyServer(9001).run)
socks_proxy.daemon=True
socks_proxy.start()

context = SecureContext(shared_secret=generate_shared_secret())
zmq_server = context.socket(zmq.REP)
zmq_client = context.socket(zmq.REQ)

# client.socks_proxy = b'127.0.0.1:9001'
zmq_client.IPV6 = 1
zmq_server.IPV6 = 1

zmq_server.bind('tcp://::1:9000')


print('about to connect...')
zmq_client.connect('socks:127.0.0.1:9001:tcp://::1:9000')

print('about to send...')
zmq_client.send(b'hello')

print('about to recv...')
print(zmq_server.recv())
