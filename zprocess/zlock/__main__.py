#####################################################################
#                                                                   #
# __main__.py                                                       #
#                                                                   #
# Copyright 2013, Chris Billington                                  #
#                                                                   #
# This file is part of the zprocess project (see                    #
# https://bitbucket.org/cbillington/zprocess) and is licensed under #
# the Simplified BSD License. See the license.txt file in the root  #
# of the project for the full license.                              #
#                                                                   #
#####################################################################
from __future__ import division, unicode_literals, print_function, absolute_import
import sys
import os
import argparse

# Ensure zprocess is in the path if we are running from this directory
if os.path.abspath(os.getcwd()) == os.path.dirname(os.path.abspath(__file__)):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.getcwd())))

import zprocess.zlock as zlock
from zprocess.zlock.server import ZMQLockServer
from zprocess.utils import disable_quick_edit


# Protocol description:
#
# Clients make requests as multipart zmq messages of bytestrings. To acquire a lock, the
# request should be:
#
# ['acquire', some_lock_key, client_id, timeout]
#
# Or, to acquire a lock read-only mode:
#
# ['acquire', some_lock_key, client_id, timeout, 'read_only']
#
# where some_lock_key is a bytestring uniquely identifying the resource that is being
# locked, client id is a bytestring uniquely identifying who is acquiring the lock, and
# timeout is (an ascii-encoded string representation of) how long (in seconds) the lock
# should be held for in the event that it is not released, say if the client process
# dies. So for example a request to lock access to a file might be:
#
# ['acquire', 'Z:\\some_folder\some_file.h5', 'hostname:process_id:thread-id', '30']
#
# The server will then block for up to zprocess.zlock.server.MAX_RESPONSE_TIME (by
# default 1 second) attempting to acquire the lock (it continues to serve other requests
# in this time, some of which may release the lock), and responds as soon as it can
# (immediately if the lock is currently free).  If it succeeds it will respond with a
# single zmq message:
#
# ['ok']
#
# If it can't acquire it after MAX_RESPONSE_TIME, it will instead respond with:
#
# ['retry']
#
# The client is free to retry immediately at that point, if it is going to retry there
# is no need to insert a delay before doing so. The absence of a delay will not create
# lots of network activity as this only happens once every MAX_RESPONSE_TIME in the case
# of ongoing lock contention. If the client does not retry with
# zprocess.zlock.server.MAX_ABSENT_TIME, it will lose its place in the queue of
# waiting clients. If the lock becomes available for the client before the client
# retries, the lock will be held for the client until it does retry, up to a maximum of
# MAX_ABSENT_TIME, after which the lock will be released and the client will lose its
# place in the queue.
#
# Anything the server replies with other than ['ok'] or ['retry'] will be a single zmq
# message and should be considered an error and raised in the client code. This will
# occur if the client provides the wrong number of messages, tries to release a lock it
# has not acquired, if it spells 'acquire' wrong or similar.
#
# To release a lock, send a three-part multipart message:
#
# ['release', some_lock_key, client_id]
#
# so for example:
#
# ['release', 'Z:\\some_folder\some_file.h5', 'hostname:process_id:thread-id']
#
# The server will respond with:
#
# ['ok']
#
# And again anything else (always a single message though) indicates an exception,
# perhaps inicating that the client releasing the lock never held it in the first place,
# or that it had expired.
#
# You can also send ['hello'], and the server will respond with ['hello'] to show that
# it is alive and working.
#
# The locking mechanism is a readers-writer lock, with preference to writers. If
# multiple clients ask to acquire the lock with 'read_only' specified ('readers'), they
# will all be granted the lock simultaneously. However, if a client not specifying
# 'read_only' (a 'writer') requests to acquire the lock, subsequent readers will not be
# granted the lock, and will have to wait. The server will then wait until all the
# existing readers have released the lock, then it will give the lock to the writer. Any
# waiting writers will be granted the lock before the waiting readers.
#
# The locks are reentrant. If a client that already holds the lock requests to acquire
# it again, the request will be granted, unless the first acquisition was read_only and
# the second is not, which results in an error. The lock must be released as many times
# as it is acquired. Re-entrant locks will time out at the latest time consistent with
# the set of requested timeouts, at which point the lock will be released completely for
# that client (i.e. not just the latest adquisition).
#

def main():

    parser = argparse.ArgumentParser(description="zprocess.zlock server.")

    parser.add_argument('-p', '--port', type=int, default=zlock.DEFAULT_PORT,
                        help='The port to listen on. Default: %d' % zlock.DEFAULT_PORT)

    parser.add_argument(
        '-a',
        '--bind-address',
        type=str,
        default='0.0.0.0',
        help="""Interface to listen on. Set to 0.0.0.0 (default) for all interfaces, or
        127.0.0.1 for localhost only.""",
    )

    parser.add_argument(
        '-s',
        '--shared-secret-file',
        type=str,
        default=None,
        help="""Filepath to the shared secret used for secure communication.""",
    )

    parser.add_argument(
        '-i',
        '--allow-insecure',
        action='store_true',
        help="""Must be set to acknowledge that communication will be insecure if
        listening on public interfaces and not using a shared secret. Currently set to
        True by default for compatibility, but this will be set to False in zprocess
        version 3.""",
    )

    args = parser.parse_args()

    port = args.port
    if args.shared_secret_file is None:
        shared_secret = None
    else:
        shared_secret = open(args.shared_secret_file).read().strip()
    allow_insecure = args.allow_insecure
    bind_address ='tcp://' + args.bind_address

    server = ZMQLockServer(
        port=port,
        bind_address=bind_address,
        shared_secret=shared_secret,
        allow_insecure=True, # TODO: deprecate in zprocess 3
    )
    disable_quick_edit()
    try:
        server.run()
    except KeyboardInterrupt:
        print('KeyboardInterrupt, stopping.', file=sys.stderr)

if __name__ == '__main__':
    main()