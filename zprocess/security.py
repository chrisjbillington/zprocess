from __future__ import print_function, unicode_literals, division
import sys
PY2 = sys.version_info[0] == 2
if PY2:
    str = unicode
import os
import uuid
import binascii
import ipaddress

import zmq
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.hmac import HMAC
from cryptography.exceptions import InvalidSignature


# A UUID to identify the zprocess encrypted message format.
ZPEM_UUID = uuid.UUID('d32a67d9-2469-4060-b843-e50f1cf1a56e').bytes


class SecurityError(RuntimeError):
    pass

class AuthenticationFailure(SecurityError):
    """An encrypted message failed authentication, indicating thet it was
    encrypted with the wrong key or corrupted/tampered with."""
    pass

class UnexpectedCiphertext(SecurityError):
    """An encrypted message was received by a socket speaking plaintext"""
    pass

class UnexpectedPlaintext(SecurityError):
    """An unencrypted message was received by a socket speaking encrypted
    messages"""
    pass

class ProtocolVersionMismatch(SecurityError):
    """An encrypted message was received with a higher major version of the
    ZPEM protocol than our version"""
    pass

class InsecureConnection(SecurityError):
    """A plaintext socket attempted to bind on an external interface or
    connect to an external address."""
    pass


def generate_preshared_key():
    """Generate a random 256 bit key appropriate for use as a preshared key,
    and return it as a hex encoded string"""
    key = os.urandom(ZProcessEncryption.CIPHER_KEYSIZE // 8)
    return binascii.hexlify(key)


class ZProcessEncryption(object):

    """Class for symmetric, authenticated encryption with a preshared key.
    Version 1.0. Message format for version 1.0 is:

    [16:UUID][1:ver_major][1:ver_minor][ciphertext][32:mac]

    where the numbers before the colons indicate the number of bytes. No
    numerical quantity is more than a byte, so we do not define endianness.
    ciphertext has arbitrary length and includes a prepended random
    initialisation vector.

    Version 1.0 uses AES 256 in CFB mode for encryption and HMACSHA256 for
    message authentication. The encryption and authentication keys are both
    derived from the preshared key by calling the HKDF key derivation function
    with SHA256 as the hash, requesting an output of 512 bits; the encryption
    key is obtained from the first 256 bits of the result and the
    authentication key from the final 256 bits. The key derivation function is
    called with only one iteration: preshared keys are expected to be
    cryptographically secure, high entropy, random data of 256 bits or more
    for which key stretching is not necessary. No salt is used in the KDF for
    the same reason.

    The MAC authenticates the entire preceding message, not just the
    ciphertext.

    The only information this scheme ought to leak if implemented correctly
    and used with a high entropy preshared key is the size of the message, the
    fact that it is a zprocess encrypted message, and the version of the
    zprocess encrypted message protocol."""

    # Message header:
    VERSION_MAJOR = 1
    VERSION_MINOR = 0

    HEADER = ZPEM_UUID + bytes(bytearray([VERSION_MAJOR, VERSION_MINOR]))

    # Key derivation:
    KDF = HKDF
    KDF_HASH = SHA256

    # Encryption:
    CIPHER = algorithms.AES
    CIPHER_KEYSIZE = 256
    CIPHER_MODE = modes.CFB
    
    # Authentication:
    HMAC_HASH = SHA256

    def __init__(self, preshared_key):
        """Set up a class for encrypting and decrypting messages using the
        zprocess encrypted message protocol. preshared_key should be a
        hex encoded string for the preshared key."""
        self.backend = default_backend()
        try:
            preshared_key_bytes = binascii.a2b_hex(preshared_key)
            if len(preshared_key_bytes) < 32:
                raise TypeError
        except (TypeError, binascii.Error):
            msg = ("preshared key must be a hex string of length at " + 
                   "least 64 (= 32 bytes = 256 bits). Use " +
                   "zprocess.security.generate_preshared_key() " +
                   "to generate random keys appropriate for use as " +
                   "preshared keys")
            if PY2:
                raise TypeError(msg)
            else:
                exec('raise TypeError(msg) from None')
        keys = self.derive_keys(preshared_key_bytes)
        self.encryption_key, self.authentication_key = keys

    def derive_keys(self, preshared_key_bytes):
        """Derive encryption and authentication keys from the preshared key"""
        kdf = self.KDF(self.KDF_HASH(),
                       self.CIPHER_KEYSIZE // 8 + self.KDF_HASH.digest_size,
                       None, None, self.backend)
        keys = kdf.derive(preshared_key_bytes)
        crypto_key = keys[:self.CIPHER_KEYSIZE // 8]
        auth_key = keys[-self.KDF_HASH.digest_size:]
        return crypto_key, auth_key

    def encrypt(self, plaintext):
        """Encrypt plaintext using the encryption key, with a random
        initialisation vector which is prepended to the ciphertext."""
        iv = os.urandom(self.CIPHER.block_size // 8)
        cipher = Cipher(self.CIPHER(self.encryption_key),
                        self.CIPHER_MODE(iv), self.backend)
        encryptor = cipher.encryptor()
        ciphertext = iv + encryptor.update(plaintext) + encryptor.finalize()
        return ciphertext

    def decrypt(self, ciphertext):
        """Decrypt the ciphertext using the encryption key"""
        iv = ciphertext[:self.CIPHER.block_size // 8]
        cipher = Cipher(self.CIPHER(self.encryption_key),
                        self.CIPHER_MODE(iv), self.backend)
        decryptor = cipher.decryptor()
        plaintext = decryptor.update(ciphertext[self.CIPHER.block_size // 8:])
        plaintext += decryptor.finalize()
        return plaintext

    def compute_mac(self, message):
        """Produce a message authentication code for a message, using the
        authentication key"""
        hmac = HMAC(self.authentication_key, self.HMAC_HASH(), self.backend)
        hmac.update(message)
        mac = hmac.finalize()
        return mac

    def verify_message(self, message):
        """Verify a message starts with the ZPEM UUID, and that the major
        version number matches ours. Verify a message by checking the message
        authentication code against the authentication key. Strip the MAC off
        and return the resulting message payload. Raises UnexpectedPlaintext
        if the message does not begin with the ZPEM UUID,
        ProtocolVersionMismatch if the message's version number does not match
        ours, and AuthenticationFailure if authentication fails. """
        if not message.startswith(ZPEM_UUID):
            msg = ("Plaintext message received on encrypted socket. " +
                   "Message was (up to 256 bytes shown):\n" +
                   message[:256].decode('ascii'))
            raise UnexpectedPlaintext(msg)

        version = bytearray(message[len(ZPEM_UUID):len(ZPEM_UUID)+2])
        version_major, version_minor = version
        if version_major != self.VERSION_MAJOR:
            sender_version = "%d.%d" % (version_major, version_minor)
            receiver_version = "%d.%d" % (self.VERSION_MAJOR, self.VERSION_MINOR)
            msg = ("Sender using protocol version (%s) " % sender_version +
                   "Incompatible with receiver (%s)" % receiver_version)
            raise ProtocolVersionMismatch(msg)

        payload = message[:-self.HMAC_HASH.digest_size]
        mac = message[-self.HMAC_HASH.digest_size:]
        hmac = HMAC(self.authentication_key, self.HMAC_HASH(), self.backend)
        hmac.update(payload)
        try:
            hmac.verify(mac)
        except InvalidSignature:
            msg = "Message failed authentication"
            if PY2:
                raise AuthenticationFailure(msg)
            else:
                exec('raise AuthenticationFailure(msg) from None')

        return payload

    def pack_message(self, plaintext):
        """Encrypt a plaintext and pack it into an authenticated ZPEM message
        ready for sending"""
        ciphertext = self.encrypt(plaintext)
        payload = self.HEADER + ciphertext
        mac = self.compute_mac(payload)
        return payload + mac

    def unpack_message(self, message):
        """Verify an encrypted message and decrypt and return the plaintext.
        Raises UnexpectedPlaintext if the message does not begin with the ZPEM
        UUID, ProtocolVersionMismatch if the message's version number does not
        match ours, and AuthenticationFailure if authentication fails."""
        payload = self.verify_message(message)
        ciphertext = payload[len(self.HEADER):]
        plaintext = self.decrypt(ciphertext)
        return plaintext


INSECURE_SEND_ERROR = ' '.join(
"""Plaintext socket send() on external network interface. This data is
unencrypted. Unless your network is trusted, use a preshared key to secure
your connection. To proceed insecurely at your own risk, use the keyword
argument allow_insecure=True to the SecureContext() or its socket()
method""".splitlines())


INSECURE_RECV_ERROR = ' '.join(
 """Plaintext socket recv() when bound on external network interface. This can
allow an attacker remote arbitrary code execution if you receive and unpickle
Python objects, and open your application to other attacks even if you do not.
Unless your network is fully trusted, use a preshared key to secure your
connection. To bind only to the local interface for connections between
processes on this computer, use the endpoint string 'tcp://127.0.0.1'. To
proceed insecurely at your own risk, use the keyword argument
allow_insecure=True to the SecureContext() or its socket()
method""".splitlines())


class SecureSocket(zmq.Socket):
    # zmq.Socket overrides __setattr__ and __getattr to set and get ZMQ
    # options, unless the name exists as a class variable. So we define dummy
    # class variables for any instance variables we want to have:
    encryption = None
    insecure = False
    allow_insecure = None
    def __init__(self, *args, **kwargs):
        """A Socket with send() and recv() methods that call
        ZProcessEncryption.pack_message() and
        ZProcessEncryption.unpack_message() to encrypt, decrypt and
        authenticate messages based on a preshared key. Accepts preshared_key
        as a keyword argument, it should be a hex encoded string of 32 bytes
        or more of cryptographically random data preshared with peers. If
        preshared_key is not provided, the preshared_key set on the Context
        will be used. In either case, if the preshared_key is None then
        unencrypted communication will occur, however send() and recv() will
        raise will raise InsecureConnection when connected/bound on interfaces
        other than localhost, this exception can be suppressed by passing
        allow_insecure=True to the socket or Context's instantiation
        kwargs."""
        preshared_key = kwargs.pop('preshared_key', self.context.preshared_key)
        self.allow_insecure = kwargs.pop('allow_insecure', self.context.allow_insecure)
        if preshared_key is not None:
            self.encryption = ZProcessEncryption(preshared_key)
        zmq.Socket.__init__(self, *args, **kwargs)

    def _is_loopback(self, endpoint):
        """Return whether a bind or connect endpoint is local"""
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

    def bind(self, addr):
        result = zmq.Socket.bind(self, addr)
        self.insecure = self.encryption is None and not self._is_loopback(addr)
        return result

    def connect(self, addr):
        result = zmq.Socket.connect(self, addr)
        self.insecure = self.encryption is None and not self._is_loopback(addr)
        return result

    def send(self, data):
        if self.encryption is not None:
            data = self.encryption.pack_message(data)
        elif self.insecure and not self.allow_insecure:
            raise InsecureConnection(INSECURE_SEND_ERROR)
        return zmq.Socket.send(self, data)

    def recv(self):
        message = zmq.Socket.recv(self)
        if self.encryption is not None:
            return self.encryption.unpack_message(message)
        elif self.insecure and not self.allow_insecure:
            raise InsecureConnection(INSECURE_RECV_ERROR)
        elif message.startswith(ZPEM_UUID):
            msg = "Encrypted message received on plaintext socket"
            raise UnexpectedCiphertext(msg)
        return message

    def send_multipart(self, parts):
        for i, data in enumerate(parts):
            if self.encryption is not None:
                data = self.encryption.pack_message(data)
            elif self.insecure and not self.allow_insecure:
                raise InsecureConnection(INSECURE_SEND_ERROR)
            if i < len(parts) - 1:
                zmq.Socket.send(self, data, flags=zmq.SNDMORE)
            else:
                zmq.Socket.send(self, data)

    def recv_multipart(self):
        # Receive them all before decrypting them, so that if
        # there is a decryption error, there are not messages
        # left in the buffer:
        parts = [zmq.Socket.recv(self)]
        # have first part already, only loop while more to receive
        while self.getsockopt(zmq.RCVMORE):
            parts.append(zmq.Socket.recv(self))
    
        plaintexts = []
        for message in parts:
            if self.encryption is not None:
                plaintext = self.encryption.unpack_message(message)
            elif self.insecure and not self.allow_insecure:
                raise InsecureConnection(INSECURE_RECV_ERROR)
            elif message.startswith(ZPEM_UUID):
                msg = "Encrypted message received on plaintext socket"
                raise UnexpectedCiphertext(msg)
            else:
                plaintext = message
            plaintexts.append(plaintext)
        return plaintexts


class SecureContext(zmq.Context):
    _socket_class = SecureSocket
    # zmq.Context overrides __setattr__ and __getattr to set and get ZMQ
    # options, unless the name exists as a class variable. So we define dummy
    # class variables for any instance variables we want to have:
    preshared_key = None
    allow_insecure = None
    def __init__(self, *args, **kwargs):
        self.preshared_key = kwargs.pop('preshared_key', None)
        self.allow_insecure = kwargs.pop('allow_insecure', False)
        zmq.Context.__init__(self, *args, **kwargs)


if __name__ == '__main__':
    import time

    key = generate_preshared_key()

    encryption = ZProcessEncryption(key)
    plaintext = b'hello'
    plaintext = os.urandom(1024**2)
    start_time = time.time()
    for i in range(10):
        message = encryption.pack_message(plaintext)
        result = encryption.unpack_message(message)
        assert result == plaintext
    print(time.time() - start_time)

    ctx = SecureContext(preshared_key=key)

    sock1 = ctx.socket(zmq.REQ, preshared_key=key)
    sock2 = ctx.socket(zmq.REP, preshared_key=key)

    port = sock2.bind_to_random_port('tcp://127.0.0.1')
    sock1.connect('tcp://localhost:%d'%port)

    import time
    start_time = time.time()
    sock1.send_multipart([b'test', b'foo'])
    print(sock2.recv_multipart())
    sock2.send(b'test')
    print(sock1.recv())

    print(time.time() - start_time)
