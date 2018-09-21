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

from zprocess.zlog import DEFAULT_PORT
from zprocess.zlog.server import (
    ZMQLogServer,
    FileHandler,
    RotatingFileHandler,
    TimedRotatingFileHandler,
)


# Protocol description:
#
# Clients should send log data as multipart messages of bytestrings from a zmq.DEALER
# socket, ensuring to prepend an empty message as is usually needed when sending from a
# DEALER. To log to a file, clients should send:
#
# ['', log', client_id, filepath, log_message]
#
# where client_id is some unique id for a client, filepath is the file to be logged to,
# and log_message is the message to write to the file, the latter two being utf8-encoded
# strings (client_id can be arbitrary bytes). The server will not respond to log
# requests, if one is malformed, or if the filepath or log message cannot be utf8
# decoded, or if the requested file cannot be written to, the zlog server will simply
# ignore the request.
#
# Other communication with the server is two-way, and can be done with a REQ socket
# instead of a DEALER if desired, in which case the initial empty messages in the below
# request specifications can be omitted (and the initial empty message in responses will
# be absent).
#
# To confirm the server is running, clients may send
#
# ['', 'hello']
#
# The server will respond with
#
# ['', 'hello']
#
# Clients can confirm that the server can open a file in append mode by sending a
# message:
#
# ['', 'check_access', filepath]
#
# The server will respond with
#
# ['', 'ok'] 
# 
# if it can open the file in append mode, or
#
# ['', error_message]
#
# if it cannot, with the error message that resulted from attempting to open the file.
# The zlog server will not open the file again until logging messages are received, and
# will close log files if no clients send data for
# zprocess.zlog.server.FILE_CLOSE_TIMEOUT, so confirming the file can be opened
# initially does not guarantee subsequent writes will succeed. Furthermore, since the
# server does not respond to log messages, there is no way for clients to guarantee in
# an ongoing way that the log messages are being written successfully.
#
# When a client is done with a log file, it should send a message:
#
# ['', 'done', client_id, filepath]
#
# The server will respond with
#
# ['', 'ok']
# 
# Once all clients writing to the same file send such a message without sending
# subsequent 'log' or 'check_access' messages, the zlog server will close the file.
# Although files will be closed anyway after a time as described above, it is good to
# send explicit 'done' messages to have the file be closed as soon as possible.
#
# clients may also request the protocol version by sending
#
# ['', 'protocol']
#
# The server will respond with
#
# ['', '1.0.0']
#
# (as of the current version).


def main():

    parser = argparse.ArgumentParser(description="zlog server.")

    parser.add_argument('-p', '--port', type=int, default=DEFAULT_PORT,
                        help='The port to listen on. Default: %d' % DEFAULT_PORT)

    parser.add_argument(
        '-c',
        '--cls',
        type=str,
        choices=['FileHandler', 'RotatingFileHandler', 'TimedRotatingFileHandler'],
        default='FileHandler',
        help="""Type of file handler to use, must be one of 'FileHandler',
            'RotatingFileHandler', or 'TimedRotatingFileHandler'. Default:
            'FileHandler'""",
    )

    parser.add_argument(
        '-n',
        '--backupCount',
        type=int,
        default=0,
        help="""If using RotatingFileHandler or TimedRotatingFileHandler, the
           value of 'backupCount' to pass to its constructor. Default: 0.""",
    )

    parser.add_argument(
        '-b',
        '--maxBytes',
        type=int,
        default=0,
        help="""If using RotatingFileHandler, the value of 'maxBytes' to pass
           to its constructor. Default: 0 (no rollover).""",
    )

    parser.add_argument(
        '-w',
        '--when',
        type=str,
        default='h',
        help="""If using TimedRotatingFileHandler, the value of 'when' to
           pass to its constructor. Default: 'h' (hours).""",
    )

    parser.add_argument(
        '-i',
        '--interval',
        type=int,
        default=1,
        help="""If using TimedRotatingFileHandler, the value of 'interval'
           to pass to its constructor. Default: 1.""",
    )

    args = parser.parse_args()

    port = args.port
    if args.cls == 'FileHandler':
        handler_class = FileHandler
        handler_kwargs = {}
    elif args.cls == 'RotatingFileHandler':
        handler_class = RotatingFileHandler
        handler_kwargs = {'maxBytes': args.maxBytes, 'backupCount': args.backupCount}
    elif args.cls == 'TimedRotatingFileHandler':
        handler_class = TimedRotatingFileHandler
        handler_kwargs = {
            'when': args.when,
            'interval': args.interval,
            'backupCount': args.backupCount,
        }
    
    server = ZMQLogServer(
        port, handler_class=handler_class, handler_kwargs=handler_kwargs
    )
    try:
        server.run()
    except KeyboardInterrupt:
        print('KeyboardInterrupt, stopping.', file=sys.stderr)
        server.context.destroy(linger=False)

if __name__ == '__main__':
    main()