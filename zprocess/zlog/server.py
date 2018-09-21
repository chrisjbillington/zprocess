from __future__ import print_function, division, absolute_import, unicode_literals
import sys
import os
import threading
import logging
import logging.handlers

import zmq

# Ensure zprocess is in the path if we are running from this directory
if os.path.abspath(os.getcwd()) == os.path.dirname(os.path.abspath(__file__)):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.getcwd())))

from zprocess.tasks import Task, TaskQueue

# If no client writes to a file in this time, we close it:
FILE_CLOSE_TIMEOUT = 5

ERR_INVALID_COMMAND = b'error: invalid command'
ERR_WRONG_NUM_ARGS = b'error: wrong number of arguments'
ERR_BAD_ENCODING = b'error: filepath not UTF8 encoded'

PROTOCOL_VERSION = '1.0.0'


def setup_logging():
    if os.name == 'nt':
        logpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'zlog.log')
    else:
        logpath = '/var/log/zlog.log'
    handlers = []
    try:
        handler = logging.handlers.RotatingFileHandler(
            logpath, maxBytes=50 * 1024 ** 2, backupCount=1
        )
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


class FileHandler(logging.FileHandler):
    instances = {}

    def __init__(self, filepath, **kwargs):
        super(FileHandler, self).__init__(filepath, **kwargs)
        self.clients = set()

    @classmethod
    def instance(cls, filepath, **kwargs):
        if filepath not in cls.instances:
            cls.instances[filepath] = cls(filepath, **kwargs)
            logging.info("Opening %s", filepath)
        return cls.instances[filepath]

    def log(self, client_id, message):
        if client_id not in self.clients:
            self.clients.add(client_id)
            msg = "New client (total: %d) for %s"
            logging.info(msg, len(self.clients), self.baseFilename)
        if hasattr(self, 'shouldRollover') and self.shouldRollover(message):
            logging.info("Rolling over %s", self.baseFilename)
            self.doRollover()
        if self.stream is None:
            try:
                self.stream = self._open()
            except OSError:
                # Nothing to be done.
                return
        try:
            self.stream.write(message + '\n')
        except OSError:
            # Nothing to be done.
            return

    def format(self, record):
        return record

    def client_done(self, client_id):
        if client_id in self.clients:
            self.clients.remove(client_id)
            msg = "Client done (remaining: %d) with %s"
            logging.info(msg, len(self.clients), self.baseFilename)
        if not self.clients:
            del self.instances[self.baseFilename]
            self.close()
            logging.info("No more clients, closing %s", self.baseFilename)


class RotatingFileHandler(FileHandler, logging.handlers.RotatingFileHandler):
    pass


class TimedRotatingFileHandler(FileHandler, logging.handlers.TimedRotatingFileHandler):
    pass


class ZMQLogServer(object):
    def __init__(
        self,
        port=None,
        bind_address='tcp://127.0.0.1',
        silent=False,
        handler_class=FileHandler,
        handler_kwargs=None,
    ):
        self.port = port
        self._initial_port = port
        self.bind_address = bind_address
        self.handler_class = handler_class
        self.handler_kwargs = handler_kwargs if handler_kwargs is not None else {}
        self.context = None
        self.router = None
        self.tasks = TaskQueue()
        self.timeout_tasks = {}

        self.run_thread = None
        self.stopping = False
        self.started = threading.Event()
        self.running = False
        self.silent = silent

    def log(self, args):
        # Validate arguments, but we can't reply if they are invalid. We just do nothing
        # in that case.
        if len(args) != 3:
            return
        client_id, filepath, message = args
        try:
            filepath = filepath.decode('utf8')
            message = message.decode('utf8')
        except UnicodeDecodeError:
            return
        handler = self.handler_class.instance(filepath, **self.handler_kwargs)
        handler.log(client_id, message)
        self.cancel_timeout(client_id)
        self.set_timeout(client_id, filepath)

    def check_access(self, routing_id, args):
        if len(args) != 1:
            self.send(routing_id, ERR_WRONG_NUM_ARGS)
            return
        filepath = args[0]
        try:
            filepath = filepath.decode('utf8')
        except UnicodeDecodeError:
            self.send(routing_id, ERR_BAD_ENCODING)
        try:
            with open(filepath, 'a') as _:
                pass
        except OSError:
            exc_class, message, _ = sys.exc_info()
            message = '%s: %s' % (exc_class.__name__, str(message))
            self.send(routing_id, message.encode('utf8'))
            logging.warning('Access denied for %s', filepath)
        else:
            self.send(routing_id, b'ok')
            logging.info('Access confirmed for %s', filepath)

    def done(self, routing_id, args):
        if len(args) != 2:
            self.send(routing_id, ERR_WRONG_NUM_ARGS)
            return
        client_id, filepath = args
        try:
            filepath = filepath.decode('utf8')
        except UnicodeDecodeError:
            self.send(routing_id, ERR_BAD_ENCODING)
            return
        self.cancel_timeout(client_id)
        try:
            handler = self.handler_class.instance(filepath, **self.handler_kwargs)
        except OSError:
            logging.warning('done() called for inaccessible file %s', filepath)
        else:
            handler.client_done(client_id)
        self.send(routing_id, b'ok')

    def set_timeout(self, client_id, filepath):
        """Add a task to say the client is done with the file after a timeout"""
        task = Task(FILE_CLOSE_TIMEOUT, self.do_timeout, client_id, filepath)
        self.tasks.add(task)
        self.timeout_tasks[client_id] = task

    def cancel_timeout(self, client_id):
        """Cancel the scheduled auto-closing of the file for the client"""
        task = self.timeout_tasks.pop(client_id, None)
        if task is not None:
            self.tasks.cancel(task)

    def do_timeout(self, client_id, filepath):
        logging.info("Client timed out for %s", filepath)
        try:
            handler = self.handler_class.instance(filepath, **self.handler_kwargs)
        except OSError:
            logging.warning('do_timeout() called for inaccessible file %s', filepath)
        else:
            handler.client_done(client_id)
        del self.timeout_tasks[client_id]

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
                if command == b'log':
                    self.log(args)
                elif command == b'hello':
                    self.send(routing_id, b'hello')
                    logging.info("Someone said hello")
                elif command == b'protocol':
                    self.send(routing_id, PROTOCOL_VERSION.encode('utf8'))
                    logging.info("Someone requested the protocol version")
                elif command == b'check_access':
                    self.check_access(routing_id, args)
                elif command == b'done':
                    self.done(routing_id, args)
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
    server = ZMQLogServer(
        port,
        handler_class=RotatingFileHandler,
        handler_kwargs={'maxBytes': 256, 'backupCount': 3},
    )
    server.run()
