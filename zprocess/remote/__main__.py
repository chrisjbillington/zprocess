from __future__ import unicode_literals, print_function, division
import sys
import locale
PY2 = sys.version_info.major == 2
if PY2:
    str = unicode
import os
import argparse

if __package__ is None:
    sys.path.insert(0, os.path.abspath('../..'))


from zprocess.remote import DEFAULT_PORT
from zprocess.remote.server import RemoteProcessServer
from zprocess.utils import disable_quick_edit


def main():

    parser = argparse.ArgumentParser(description="zprocess.remote server.")

    parser.add_argument('-p', '--port', type=int, default=DEFAULT_PORT,
                        help='The port to listen on. Default: %d' % DEFAULT_PORT)

    parser.add_argument(
        '-a',
        '--bind-address',
        type=str,
        default='0.0.0.0',
        help="""Interface to listen on. Set to 0.0.0.0 (default) for all interfaces, or
        127.0.0.1 for localhost only.""",
    )

    exclusive_grp = parser.add_mutually_exclusive_group()

    exclusive_grp.add_argument(
        '-i',
        '--allow-insecure',
        action='store_true',
        dest='allow_insecure',
        default=True, # TODO: default to False in zprocess 3.0
        help="""Must be set to acknowledge that communication will be insecure if not
        using a shared secret, otherwise connections to hosts other than localhost will
        raise an exception. Is by default set on zprocess 2 for backward compatibility,
        but will be unset by default in zprocess 3.""",
    )

    exclusive_grp.add_argument(
        '-ni',
        '--no-allow-insecure',
        action='store_false',
        dest='allow_insecure',
        help="""Set to explicitly disallow insecure connections. This will be
        unnecessary on zprocess 3.0, when requiring secure connections will be the
        default behaviour.""",
    )

    parser.add_argument(
        '-s',
        '--shared-secret-file',
        type=str,
        default=None,
        help="""Filepath to the shared secret used for secure communication.""",
    )

    parser.add_argument(
        '-tui',
        '--text-interface',
        action='store_true',
        help="""Run as a text-based interface showing subprocesses and clients""",
    )

    args = parser.parse_args()

    port = args.port
    if args.shared_secret_file is None:
        shared_secret = None
    else:
        shared_secret = open(args.shared_secret_file).read().strip()

    if args.shared_secret_file is None:
        shared_secret = None
    else:
        shared_secret = open(args.shared_secret_file).read().strip()
    allow_insecure = args.allow_insecure
    bind_address ='tcp://' + args.bind_address

    def run_curses(stdscr):
        import curses
        from zprocess.remote.curses_server import RemoteProcessServerCurses
    
        curses.curs_set(False)
        curses.halfdelay(1)
        stdscr.clear()
        stdscr.refresh()
        curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)
        curses.init_pair(2, curses.COLOR_WHITE, curses.COLOR_RED)
        curses.init_pair(3, curses.COLOR_WHITE, curses.COLOR_YELLOW)
        server = RemoteProcessServerCurses(
            stdscr,
            port=port,
            bind_address=bind_address,
            shared_secret=shared_secret,
            allow_insecure=allow_insecure,
        )
        server.shutdown_on_interrupt()

    disable_quick_edit()

    if args.text_interface:
        import curses
    
        locale.setlocale(locale.LC_ALL, '')
        curses.wrapper(run_curses)
    else:
        server = RemoteProcessServer(
            port=port,
            bind_address=bind_address,
            shared_secret=shared_secret,
            allow_insecure=allow_insecure,
        )
        server.shutdown_on_interrupt()


if __name__ == '__main__':
    main()
