from __future__ import print_function, unicode_literals, division, absolute_import
import sys
import ipaddress
from struct import pack, unpack
from binascii import hexlify, unhexlify
import enum

PY2 = sys.version_info.major == 2
if PY2:
    str = unicode
    from StringIO import StringIO as BytesIO
else:
    from io import BytesIO

DEFAULT_PORT = 7342

# SOCKS 5 protocol constants:
class socks5(enum.IntEnum):

    VERSION = 0x05

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

    # Status
    GRANTED = 0x00
    FAILURE = 0x01
    DENIED = 0x02
    NETWORK_UNREACHABLE = 0x03
    HOST_UNREACHABLE = 0x04
    CONNECTION_REFUSED = 0x05
    TTL_EXPIRED = 0x06
    COMMAND_NOT_SUPPORTED_OR_PROTOCOL_ERROR = 0x07
    ADDRESS_TYPE_NOT_SUPPORTED = 0x08


def parse_address(readfunc):
    """Parse an address in binary SOCKS 5 format given a readfunc(n) which reliably
    returns the number of bytes requested. Returns the address_type, the address as
    a string, port number as an int, and the bytes of the binary message that was
    read. Stops reading and returns None for the address and port if the address
    type was not one of IPV4, IPV6 or DOMAIN"""
    address_message = readfunc(1)
    address_type, = unpack('B', address_message)
    if address_type == socks5.IPV4:
        raw_address = readfunc(4)
        address = ipaddress.IPv4Address(raw_address).compressed
    elif address_type == socks5.IPV6:
        raw_address = readfunc(16)
        address = ipaddress.IPv6Address(raw_address).compressed
    elif address_type == socks5.DOMAIN:
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


def pack_address(host, port):
    """Pack an IP address and port into a binary message for the SOCKS 5 protocol"""
    ip = ipaddress.ip_address(host)
    if ip.version == 4:
        msg = pack('B', socks5.IPV4)
    elif ip.version == 6:
        msg = pack('B', socks5.IPV6)
    else:
        assert False
    msg += ip.packed + pack('!H', port)
    return msg


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
    a zeromq connect() call (does not include the 'tcp://'' prefix)"""
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


def hops_to_domain_addr(hops):
    """Take a list of (host, port) tuples as returned by str_to_hops(), and
    return a domain name and port, where the domain name encodes all the hops required
    to get to the final host, and the port is simply the port of the final hop.

    The domain is a hex encoded string of the following bytes:
        [num_hops]: 1 byte
        then for each hop:
            [addr_type]: 1 byte, IPV4 or IPV6 as defined in SOCKS 5
            [addr]: the IP address as a network-order integer, 4 bytes for IPV4 and 16
                    bytes for IPV6
            [port]: network order integer, 2 bytes

    with the final hop lacking the port field. In this way, a multihop request can be
    encoded as a single domain and port.

    If there is only one hop, the host will be returned as is without any encoding."""
    if not hops:
        raise ValueError("no hops")
    if len(hops) == 1:
        return hops[0]
    packed_hops = pack('B', len(hops))
    for host, port in hops:
        ip = ipaddress.ip_address(host)
        if ip.version == 4:
            packed_hops += pack('B', socks5.IPV4)
        elif ip.version == 6:
            packed_hops += pack('B', socks5.IPV6)
        else:
            raise ValueError(ip.version)
        packed_hops += ip.packed
        packed_hops += pack('!H', int(port))
    return hexlify(packed_hops[:-2]).decode(), port


def domain_addr_to_hops(domain, port):
    """Unpack a multihop request encoded as a domain and port, as returned by
    hops_to_domain_addr(), into a list of tuples of ip addresses and ports"""

    # Add the final port to the packed data so it can all be parsed the same way:
    readfunc = BytesIO(unhexlify(domain) + pack('!H', port)).read
    n_hops, = unpack('B', readfunc(1))
    hops = []
    for _ in range(n_hops):
        addr_type, address, port, _ = parse_address(readfunc)
        if addr_type not in [socks5.IPV4, socks5.IPV6]:
            raise ValueError("Invalid addr_type %s" % addr_type)
        hops.append((address, port))
    return hops
