import sys
import curses
import locale
import threading
import datetime
import collections
import textwrap
from zprocess.remote.server import RemoteProcessServer


def ignore_curses_err(func):
    def f_new(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except curses.error:
            return None
    return f_new


class OutputCapturer(object):
    """File-like object to replace sys.stdout and sys.stderr whilst the curses interface
    is running, so that we can print log lines to the curses screen."""
    def __init__(self, server, streamname):
        self.server = server
        self.streamname = streamname
        self.stream = getattr(sys, streamname)

    def write(self, data):
        with self.server.lock:
            self.server.loglines.append(data)
            self.server.update_screen()
        return self.stream.write(data)

    def flush(self):
        return self.stream.flush()

    def fileno(self):
        return self.stream.fileno()

    def isatty(self):
        return self.stream.isatty()


class LogLines(object):
    """A ring-buffer of log lines to be displayed in the curses interface"""
    def __init__(self, maxlines):
        self.maxlines = maxlines
        self.lines = collections.deque(maxlen=100)
        self.lines.append('')

    def append(self, data):
        lines = data.split('\n')
        self.lines[-1] += lines[0]
        self.lines.extend(lines[1:])

    def get_tail(self, height, width):
        wrapped_lines = []
        for line in self.lines:
            wrapped_lines.extend(textwrap.wrap(line, width))
        return wrapped_lines[-height:]


CLIENT_LIST = 0
SPLIT_VIEW = 1
LOG_VIEW = 2


class RemoteProcessServerCurses(RemoteProcessServer):

    def __init__(self, stdscr, *args, **kwargs):
        self.stdscr = stdscr
        self.addstr = ignore_curses_err(self.stdscr.addstr)
        self.addch = ignore_curses_err(self.stdscr.addch)
        self.lock = threading.RLock()
        self.currpos = [0, 0]
        self.linelen_max = 0
        self.encoding = locale.getpreferredencoding()
        self.loglines = LogLines(100)
        self.tab = SPLIT_VIEW
        sys.stdout = OutputCapturer(self, 'stdout')
        sys.stderr = OutputCapturer(self, 'stderr')
        RemoteProcessServer.__init__(self, *args, **kwargs)
        curses.use_default_colors()
        self.update_screen()

    def handler(self, *args, **kwargs):
        with self.lock:
            try:
                result = RemoteProcessServer.handler(self, *args, **kwargs)
            finally:
                self.update_screen()
            return result

    def timeout(self):
        with self.lock:
            RemoteProcessServer.timeout(self)
            self.update_screen()

    def shutdown_on_interrupt(self):
        """ Handles key presses. Scroll the screen with arrow keys.
            Press C, or ctrl-C to quit. Press L to toggle log visibility"""
        try:
            while True:
                scr_height, scr_width = self.stdscr.getmaxyx()
                xstep = scr_width // 2
                c = self.stdscr.getch()
                with self.lock:
                    if c == ord('q') or c == 3 :# Ctrl-C
                        raise KeyboardInterrupt
                    if c == ord('\t'):
                        self.tab = (self.tab + 1) % 3
                    elif c == curses.KEY_LEFT:
                        self.currpos[1] = max(self.currpos[1]-xstep, 0)
                    elif c == curses.KEY_RIGHT:
                        self.currpos[1] = self.currpos[1]+xstep
                    elif c == curses.KEY_UP or c == ord('k'):
                        self.currpos[0] = max(self.currpos[0]-1, 0)
                    elif c == curses.KEY_DOWN or c == ord('j'):
                        self.currpos[0] = self.currpos[0]+1
                    elif c == ord('f'):
                        self.currpos[0] = self.currpos[0] + scr_height - 4
                    elif c == ord('b'):
                        self.currpos[0] = self.currpos[0] - (scr_height - 4)
                    self.update_screen()
        except KeyboardInterrupt:
            sys.stderr.write('Interrupted, shutting down\n')
        finally:
            self.shutdown()

    def get_lines(self):
        """ Generate lines to be printed of subprocesses, sorted by parent."""
        lines = []

        for client_ip in set(self.parents.values()):
            style = curses.color_pair(1) | curses.A_BOLD
            lines.append([['*', style], ' Client: {}'.format(client_ip)])

            pids = {pid for pid in self.children if self.parents[pid] == client_ip}

            for i, pid in enumerate(sorted(pids)):
                if i < len(pids) - 1:
                    thead = [curses.ACS_LTEE, ' ']
                else:
                    thead = [curses.ACS_LLCORNER, ' ']
                args = '"' + ' '.join(self.children[pid].args) + '"'
                rc = self.children[pid].poll()
                if pid not in self.orphans:
                    if rc is None:
                        # Process is alive
                        lines.append(thead + [['*', curses.color_pair(1) | curses.A_BOLD], ' pid: {} {}'.format(pid, args)])
                    else:
                        # Process is dead.
                        lines.append(thead + [['* pid: {} {}'.format(pid, args), curses.color_pair(2) | curses.A_BOLD]])
                else:
                    # Process is alive, but the parent has deleted its reference to it.
                    lines.append(thead + [['* pid: {} {}'.format(pid, args), curses.color_pair(3) | curses.A_BOLD]])

        return lines

    def update_screen(self):
        """ Redraw the screen given the lines generated by get_lines(). """
        clients = set(self.parents.values())
        scr_height, scr_width = self.stdscr.getmaxyx()
        self.stdscr.clear()
        self.addstr(0, 0, 'Control port {}'.format(self.port))
        self.addstr(1, 0, 'Legend:'+' '*25)
        self.addstr(1, 10, 'normal', curses.color_pair(1) | curses.A_BOLD)
        self.addstr(1, 18, 'orphaned', curses.color_pair(3) | curses.A_BOLD)
        self.addstr(1, 28, 'dead', curses.color_pair(2) | curses.A_BOLD)
        self.addstr(2, 0, 'Press Q or Ctrl-C to quit, <tab> to switch view')
        self.addstr(3, 0, 'Total clients: {}'.format(len(clients)), curses.A_BOLD)
        tab_width = 13
        tabs = ['Client list', 'Split view', 'Log']
        tab_pos = (scr_width - tab_width * len(tabs)) // 2
        self.addstr(4, 0, '-' * scr_width)
        for i, tab in enumerate(tabs):
            style = curses.A_REVERSE
            if self.tab == tabs.index(tab):
                style |= curses.A_BOLD
            self.addstr(4, tab_pos + i * tab_width, tab.center(tab_width), style)

        ymin = 5
        ymax = scr_height - 2
        if scr_height-1 >= ymin:
            self.addstr(scr_height-1, 0, datetime.datetime.now().strftime('%c'), curses.A_REVERSE)
        if self.tab == LOG_VIEW:
            nloglines = (ymax - ymin)
        elif self.tab == SPLIT_VIEW:
            nloglines = (ymax - ymin) // 2
        else:
            nloglines = 0
        if nloglines:
            log_start_pos = scr_height - 3 - nloglines
            if self.tab == SPLIT_VIEW:
                self.addstr(log_start_pos, 0, '-' * scr_width)
            loglines = self.loglines.get_tail(nloglines, scr_width)
            for i, line in enumerate(loglines):
                self.addstr(log_start_pos + 1 + i, 0, line)
        if self.tab == LOG_VIEW:
            return
        lines = self.get_lines()
        if nloglines:
            ymax = log_start_pos - 1
        # restrict scroll area
        self.currpos[0] = max(min(self.currpos[0], max(len(lines) - (ymax - ymin + 1), 0)), 0)
        self.currpos[1] = max(min(self.currpos[1], max(self.linelen_max - scr_width, 0)), 0)
        # display relative position
        s = str(self.currpos)
        if scr_height-1 >= ymin:
            self.addstr(scr_height-1, scr_width-len(s), s, curses.A_REVERSE)
        # print data
        y = ymin - self.currpos[0] - 1
        self.linelen_max = 0
        for l in lines:
            y += 1
            if y < ymin:
                continue
            if y > ymax:
                continue
            x = - self.currpos[1]
            linelen = 0
            for item in l:
                if not isinstance(item, list):
                    item = [item, None] # string and attr
                s = item[0]
                attr = item[1]
                if isinstance(s, int):
                    linelen += 1
                    if x < 0:
                        x += 1
                        continue
                    if x >= scr_width:
                        continue
                    if attr is None:
                        self.addch(y, x, s)
                    else:
                        self.addch(y, x, s, attr)
                    x += 1
                else:
                    linelen += len(s)
                    if x < 0:
                        if x + len(s) <= 0:
                            x += len(s)
                            continue
                        else:
                            s = s[-x:]
                            x = 0
                    if x >= scr_width:
                        continue
                    if x + len(s) >= scr_width:
                        s = s[:scr_width-x]
                    slen = len(s)
                    if isinstance(s, str):
                        s = s.encode(self.encoding)
                    if attr is None:
                        self.addstr(y, x, s)
                    else:
                        self.addstr(y, x, s, attr)
                    x += slen
            self.linelen_max = max(self.linelen_max, linelen)
        self.stdscr.refresh()