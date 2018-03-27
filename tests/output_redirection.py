from __future__ import absolute_import, unicode_literals, print_function, division
import sys
PY2 = sys.version_info.major == 2
import os
import time
import threading
import subprocess
import zmq


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

from zprocess.security import SecureContext
from zprocess.process_tree import OutputInterceptor

context = SecureContext()
sock = context.socket(zmq.PULL)
port = sock.bind_to_random_port('tcp://127.0.0.1')
print('pre-redirect: hello!')
if sys.stdout is not None:
    print('pre-redirect: stdout is a tty:', sys.stdout.isatty())
interceptor = OutputInterceptor('localhost', port, 'stdout')
interceptor2 = OutputInterceptor('localhost', port, 'stderr')
interceptor.connect()
interceptor2.connect()
try:
    for i in range(3):
        print("python hello")
        sys.stdout.write('1\n')
        sys.stderr.write('2\n')
        os.system('echo echo hello')

    child = subprocess.Popen(['python', '-c',
                             'import time; time.sleep(2); print("hello")'])
    del child
    print('stdout is a tty:', sys.stdout.isatty())
finally:
    interceptor.disconnect()
    interceptor2.disconnect()
    print('post-redirect: hello!')
    if sys.stdout is not None:
        print('stdout is a tty:', sys.stdout.isatty())

    if sys.stdout is None or sys.stdout.fileno() < 0:
        f = open('test.txt', 'w')
        g = f
    else:
        f = sys.stdout
        g = sys.stderr
    while sock.poll(100):
        data = sock.recv_multipart()
        if data[0] == b'stdout':
            f.write(data[1].decode())
        else:
            g.write(data[1].decode())
    f.flush()
    g.flush()