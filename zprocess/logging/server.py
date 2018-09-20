from __future__ import print_function, division, absolute_import, unicode_literals
import sys
import os
import threading
import logging
from logging.handlers import RotatingFileHandler

import zmq

# Ensure zprocess is in the path if we are running from this directory
if os.path.abspath(os.getcwd()) == os.path.dirname(os.path.abspath(__file__)):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.getcwd())))

from zprocess.tasks import Task, TaskQueue

# If no client writes to a file in this time, we close it:
FILE_CLOSE_TIMEOUT = 15

ERR_INVALID_COMMAND = b'error: invalid command'
ERR_WRONG_NUM_ARGS = b'error: wrong number of arguments'

PROTOCOL_VERSION = '1.0.0'


def setup_logging():
    if os.name == 'nt':
        logpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'zlog.log')
    else:
        logpath = '/var/log/zlog.log'
    handlers = []
    try:
        handler = RotatingFileHandler(logpath, maxBytes=50 * 1024 ** 2, backupCount=1)
        handlers.append(handler)
        file_handler_success = True
    except IOError:
        file_handler_success = False
    if sys.stdout is not None and sys.stdout.isatty():
        handlers.append(logging.StreamHandler(sys.stdout))
    logging.basicConfig(
        format='[%(asctime)s] %(levelname)s: %(message)s',
        level=logging.DEBUG,
        handlers=handlers,
    )
    if not file_handler_success:
        msg = 'Can\'t open or do not have permission to write to log file '
        msg += logpath + '. Only terminal logging will be output.'
        logging.warning(msg)


class ZMQLogServer(object):
    def __init__(self, port=None, bind_address='tcp://0.0.0.0', silent=False):
        self.port = port
        self._initial_port = port
        self.bind_address = bind_address
        self.context = None
        self.router = None
        self.tasks = TaskQueue()
        self.active_locks = {}

        self.run_thread = None
        self.stopping = False
        self.started = threading.Event()
        self.running = False
        self.silent = silent

    def run(self):
        self.context = zmq.Context.instance()
        self.router = self.context.socket(zmq.ROUTER)
        poller = zmq.Poller()
        poller.register(self.router, zmq.POLLIN)
        if self.port is not None:
            self.router.bind('%s:%d' % (self.bind_address, self.port))
        else:
            self.port = self.router.bind_to_random_port(self.bind_address)
        if not self.silent:  # pragma: no cover
            setup_logging()
            msg = 'This is zlog server, running on %s:%d'
            logging.info(msg, self.bind_address, self.port)
        self.running = True
        self.started.set()
        while True:
            # Wait until we receive a request or a task is due:
            if self.tasks:
                timeout = max(0, 1000 * self.tasks.next().due_in())
            else:
                timeout = None
            events = poller.poll(timeout)
            if events:
                # A request was received:
                request = self.router.recv_multipart()

                if len(request) < 3 or request[1] != b'':  # pragma: no cover
                    # Not well formed as [routing_id, '', command, ...]
                    continue  # pragma: no cover
                routing_id, command, args = request[0], request[2], request[3:]
                if command == b'hello':
                    self.send(routing_id, b'hello')
                    logging.info("Someone said hello")
                elif command == b'protocol':
                    self.send(routing_id, PROTOCOL_VERSION.encode('utf8'))
                    logging.info("Someone requested the protocol version")
                elif command == b'stop' and self.stopping:
                    self.send(routing_id, b'ok')
                    break
                else:
                    self.send(routing_id, ERR_INVALID_COMMAND)
            else:
                # A task is due:
                task = self.tasks.pop()
                task()
        self.router.close()
        self.router = None
        self.context = None
        self.port = self._initial_port
        self.running = False
        self.started.clear()

    def run_in_thread(self):
        """Run the main loop in a separate thread, returning immediately"""
        self.run_thread = threading.Thread(target=self.run)
        self.run_thread.daemon = True
        self.run_thread.start()
        if not self.started.wait(timeout=2):
            raise RuntimeError('Server failed to start')  # pragma: no cover

    def stop(self):
        if not self.running:
            raise RuntimeError('Not running')
        self.stopping = True
        sock = self.context.socket(zmq.REQ)
        sock.connect('tcp://127.0.0.1:%d' % self.port)
        sock.send(b'stop')
        assert sock.recv() == b'ok'
        sock.close()
        if self.run_thread is not None:
            self.run_thread.join()
        self.stopping = False

    def send(self, routing_id, message):
        self.router.send_multipart([routing_id, b'', message])


if __name__ == '__main__':
    port = 7340
    server = ZMQLogServer(port)
    server.run()
