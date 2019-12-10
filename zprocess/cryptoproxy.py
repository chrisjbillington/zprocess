from __future__ import division, unicode_literals, print_function, absolute_import
import os
import sys
import argparse

# Ensure zprocess is in the path if we are running from this directory
if os.path.abspath(os.getcwd()) == os.path.dirname(os.path.abspath(__file__)):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.getcwd())))

from zprocess.security import SecureContext
import zmq


def main():
    parser = argparse.ArgumentParser(
        description="""zprocess.cryptoproxy - listen on localhost for unencrypted
            connections and securely forward them to a server using the given shared
            secret."""
    )

    parser.add_argument(
        'hostname', type=str, help="""Host or IP of server to connect to""",
    )

    parser.add_argument(
        'server_port',
        metavar='server-port',
        type=str,
        help="""Server port to connect to""",
    )

    parser.add_argument(
        'shared_secret_file',
        metavar='shared-secret-file',
        type=str,
        help="""Filepath to the shared secret used for secure communication""",
    )

    parser.add_argument(
        '-p',
        '--listen-port',
        type=str,
        help="""Port onto listen on on localhost. Defaults to server port""",
    )

    args = parser.parse_args()

    with open(args.shared_secret_file) as f:
        shared_secret = f.read()

    secure_ctx = SecureContext(shared_secret=shared_secret)
    ctx = zmq.Context()
    client_facing = ctx.socket(zmq.ROUTER)
    server_facing = secure_ctx.socket(zmq.DEALER)

    server_facing.connect('tcp://%s:%s' % (args.hostname, args.server_port))
    client_facing.bind('tcp://127.0.0.1:%s' % args.listen_port)
    zmq.proxy(client_facing, server_facing)


if __name__ == '__main__':
    main()
