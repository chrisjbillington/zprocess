from __future__ import print_function, unicode_literals

import sys
import os

if 'pythonw.exe' in sys.executable.lower():
    import subprocess
    # Re-launch with python.exe and hidden console window:
    CREATE_NO_WINDOW = 1 << 27
    cmd = [sys.executable.lower().replace('pythonw.exe', 'python.exe')] + sys.argv
    proc = subprocess.Popen(cmd, creationflags=CREATE_NO_WINDOW)
    sys.exit(0)


import os
from zprocess.process_tree import OutputInterceptor
from qtutils.outputbox import OutputBox
from qtutils.qt import QtGui, QtWidgets

import ctypes
if os.name == 'nt':
    libc = ctypes.cdll.msvcrt
else:
    libc = ctypes.CDLL(None)

import multiprocessing


def regular_print_stdout(button):
    print("hello from a regular print to stdout")
    # print("    stdout is:", sys.stdout, "fileno:", sys.stdout.fileno())
    # print("    orig stdout is:", sys.__stdout__, "fileno:", sys.__stdout__.fileno())

def regular_print_stderr(button):
    print("hello from a regular print to stderr", file=sys.stderr)
    # print("    stderr is:", sys.stderr, "fileno:", sys.stderr.fileno(), file=sys.stderr)
    # print("    orig stderr is:", sys.__stderr__, "fileno:", sys.__stderr__.fileno(), file=sys.stderr)

def libc_printf(button):
    libc.printf(b"hello from printf to stdout via libc\n")

def echo_hello(button):
    os.system("echo hello from echo via os.system")

def multiprocessing_Process(button):
    if os.name != 'nt' and sys.version_info.major == 2:
        msg = (
            "Cannot fork processes when using Qt. This test only works on Windows "
            + "or Python 3 where forking can be disabled."
        )
        print(msg, file=sys.stderr)
        return
    if multiprocessing.get_start_method(True) != 'spawn':
        multiprocessing.set_start_method('spawn')
    proc = multiprocessing.Process(
        target=print, args=('hello from print() in a multiprocessing.Process()',)
    )

    proc.start()
    proc.join()

def main():
    app = QtWidgets.QApplication(sys.argv)
    window = QtWidgets.QWidget()
    layout = QtWidgets.QVBoxLayout(window)
    outputbox = OutputBox(layout)

    funcs = [
        regular_print_stdout,
        regular_print_stderr,
        libc_printf,
        echo_hello,
        multiprocessing_Process,
    ]

    for f in funcs:
        button = QtWidgets.QPushButton(f.__name__)
        button.clicked.connect(f)
        layout.addWidget(button)

    redirect_stdout = OutputInterceptor('localhost', outputbox.port)
    redirect_sterr = OutputInterceptor('localhost', outputbox.port, streamname='stderr')

    redirect_stdout.connect()
    redirect_sterr.connect()

    window.resize(800, 500)
    window.show()
    app.exec_()
    redirect_stdout.disconnect()
    redirect_sterr.disconnect()
    outputbox.shutdown()

if __name__ == '__main__':
    main()