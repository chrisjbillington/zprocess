from __future__ import unicode_literals, print_function, division
import sys
PY2 = sys.version_info.major == 2
if PY2:
    str = unicode
import os
import argparse

if __package__ is None:
    sys.path.insert(0, os.path.abspath('../..'))


from zprocess.remote import DEFAULT_PORT
from zprocess.remote.server import RemoteProcessServer

def main():

    parser = argparse.ArgumentParser(description="zprocess.remote server.")

    parser.add_argument('-p', '--port', type=int, default=DEFAULT_PORT,
                        help='The port to listen on. Default: %d' % DEFAULT_PORT)

    parser.add_argument(
        '-i',
        '--allow-insecure',
        action='store_true',
        help="""Must be set to acknowledge that communication will be insecure if not
        using a shared secret.""",
    )

    parser.add_argument(
        '-s',
        '--shared-secret-file',
        type=str,
        default=None,
        help="""Filepath to the shared secret used for secure communication.""",
    )
    args = parser.parse_args()

    port = args.port
    if args.shared_secret_file is None:
        shared_secret = None
    else:
        shared_secret = open(args.shared_secret_file).read().strip()
    allow_insecure = args.allow_insecure

    if not allow_insecure and shared_secret is None:
        parser.error('Must either provide shared secret file or specify --allow-insecure.')

    server = RemoteProcessServer(
        port=port, shared_secret=shared_secret, allow_insecure=allow_insecure
    )
    server.shutdown_on_interrupt()


if __name__ == '__main__':
    main()
