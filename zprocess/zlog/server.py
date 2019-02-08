from __future__ import print_function, division, absolute_import, unicode_literals
import sys
import os
import traceback
import threading
import logging
import logging.handlers
import uuid

import zmq
from zmq.utils.win32 import allow_interrupt

# Ensure zprocess is in the path if we are running from this directory
if os.path.abspath(os.getcwd()) == os.path.dirname(os.path.abspath(__file__)):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.getcwd())))

from zprocess.tasks import Task, TaskQueue
from zprocess.security import SecureContext
from zprocess.utils import setup_logging

# If no client writes to a file in this time, we close it:
FILE_CLOSE_TIMEOUT = 5

ERR_INVALID_COMMAND = b'error: invalid command'
ERR_WRONG_NUM_ARGS = b'error: wrong number of arguments'
ERR_BAD_ENCODING = b'error: filepath not UTF8 encoded or contains nulls'
PROTOCOL_VERSION = '1.0.0'


def _format_exc():
    """Format just the last line of the current exception, not the whole traceback"""
    exc_type, exc_value, _ = sys.exc_info()
    return traceback.format_exception_only(exc_type, exc_value)[0].strip()


class FileHandler(logging.FileHandler):
    instances = {}

    def __init__(self, filepath, **kwargs):
        # Don't open the file until logging is requested:
        kwargs['delay'] = True
        super(FileHandler, self).__init__(filepath, **kwargs)
        self.clients = set()

    @classmethod
    def instance(cls, filepath, **kwargs):
        if filepath not in cls.instances:
            cls.instances[filepath] = cls(filepath, **kwargs)
            logger.info("Opening %s", filepath)
        return cls.instances[filepath]

    def log(self, client_id, message):
        if client_id not in self.clients:
            self.clients.add(client_id)
            msg = "New client (total: %d) for %s"
            logger.info(msg, len(self.clients), self.baseFilename)
        if hasattr(self, 'shouldRollover'):
            try:
                if self.shouldRollover(message):
                    logging.info("Rolling over %s", self.baseFilename)
                    self.doRollover()
            except (OSError, IOError):
                logger.warning(
                    "Failed to check/rollover %s:\n    %s",
                    self.baseFilename,
                    _format_exc(),
                )
                return
        if self.stream is None:
            try:
                self.stream = self._open()
            except (OSError, IOError):
                logger.warning(
                    "Failed to open %s:\n    %s", self.baseFilename, _format_exc()
                )
                return
        try:
            self.stream.write(message + '\n')
        except (OSError, IOError):
            # Nothing to be done.
            return

    def format(self, record):
        return record

    def client_done(self, client_id, ip=None):
        if client_id in self.clients:
            self.clients.remove(client_id)
            msg = "Client done (remaining: %d) with %s"
            if ip is not None:
                msg = '[%s] ' % ip + msg
            logger.info(msg, len(self.clients), self.baseFilename)
        if not self.clients:
            del self.instances[self.baseFilename]
            if self.stream is not None:
                logger.info("No more clients, closing %s", self.baseFilename)
                self.close()


class RotatingFileHandler(FileHandler, logging.handlers.RotatingFileHandler):
    pass


class TimedRotatingFileHandler(FileHandler, logging.handlers.TimedRotatingFileHandler):
    pass


class ZMQLogServer(object):
    def __init__(
        self,
        port=None,
        bind_address='tcp://0.0.0.0',
        silent=False,
        handler_class=FileHandler,
        handler_kwargs=None,
        shared_secret=None,
        allow_insecure=True
    ):
        self.port = port
        self._initial_port = port
        self.bind_address = bind_address
        self.handler_class = handler_class
        self.shared_secret = shared_secret
        self.allow_insecure = allow_insecure
        self.handler_kwargs = handler_kwargs if handler_kwargs is not None else {}
        self.context = None
        self.router = None
        self.poller = None
        self.tasks = TaskQueue()
        self.timeout_tasks = {}

        self.run_thread = None
        self.stopping = False
        self.started = threading.Event()
        self.running = False
        self.silent = silent
        self._interrupted = False

    def log(self, args):
        # Validate arguments, but we can't reply if they are invalid. We just do nothing
        # in that case.
        if len(args) != 3:
            return
        client_id, filepath, message = args
        filepath = self._check_filepath(filepath)
        if filepath is None:
            return
        try:
            message = message.decode('utf8')
        except UnicodeDecodeError:
            return
        filepath = os.path.abspath(filepath)
        handler = self.handler_class.instance(filepath, **self.handler_kwargs)
        handler.log(client_id, message)
        self.cancel_timeout(client_id)
        self.set_timeout(client_id, filepath)

    def _check_filepath(self, filepath):
        """UTF8-decode a filepath, convert it to an absolute filepath, check it has no
        nulls, and return the result. If the filepath is invalid (can't be decoded or
        has nulls), returns None."""
        if b'\0' in filepath:
            return
        try:
            filepath = filepath.decode('utf8')
        except UnicodeDecodeError:
            return
        return os.path.abspath(filepath)

    def check_access(self, routing_id, ip, args):
        if len(args) != 1:
            self.send(routing_id, ERR_WRONG_NUM_ARGS)
            return
        filepath = self._check_filepath(args[0])
        if filepath is None:
            self.send(routing_id, ERR_BAD_ENCODING)
            return
        dirname = os.path.dirname(filepath)
        try:
            # Check we can open the file in append mode:
            with open(filepath, 'a') as _:
                pass
            if self.handler_class in [RotatingFileHandler, TimedRotatingFileHandler]:
                # Check we can create files in the directory, so that we can make backup
                # log files in the directory (not a guarantee obviously, but pretty
                # good):
                test_filename = 'test_can_create_file-' + uuid.uuid4().hex
                test_filepath = os.path.join(dirname, test_filename)
                try:
                    with open(test_filepath, 'w'):
                        pass
                finally:
                    try:
                        os.unlink(test_filepath)
                    except (OSError, IOError):
                        pass
        except (OSError, IOError):
            message = _format_exc()
            self.send(routing_id, message.encode('utf8'))
            msg = '[%s] Access denied for %s: \n    %s'
            logger.warning(msg, ip, filepath, message)
        else:
            self.send(routing_id, b'ok')
            logger.info('[%s] Access confirmed for %s', ip, filepath)

    def done(self, routing_id, ip, args):
        if len(args) != 2:
            self.send(routing_id, ERR_WRONG_NUM_ARGS)
            return
        client_id, filepath = args
        filepath = self._check_filepath(filepath)
        if filepath is None:
            self.send(routing_id, ERR_BAD_ENCODING)
            return
        self.cancel_timeout(client_id)
        handler = self.handler_class.instance(filepath, **self.handler_kwargs)
        handler.client_done(client_id, ip=ip)
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
        logger.info("Client timed out for %s", filepath)
        handler = self.handler_class.instance(filepath, **self.handler_kwargs)
        handler.client_done(client_id)
        del self.timeout_tasks[client_id]

    def run(self):
        self.context = SecureContext.instance(shared_secret=self.shared_secret)
        self.router = self.context.socket(
            zmq.ROUTER, allow_insecure=self.allow_insecure
        )
        self.poller = zmq.Poller()
        self.poller.register(self.router, zmq.POLLIN)
        if self.port is not None:
            self.router.bind('%s:%d' % (self.bind_address, self.port))
        else:
            self.port = self.router.bind_to_random_port(self.bind_address)
        global logger
        logger = setup_logging('zlog', self.silent)
        if not self.silent:
            # Log insecure connection attempts:
            self.router.logger = logger
        msg = 'This is zlog server, running on %s:%d'
        logger.info(msg, self.bind_address, self.port)
        self.running = True
        self.started.set()
        try:
            with allow_interrupt(self._win32_keyboardinterrupt):
                try:
                    self._mainloop()
                except KeyboardInterrupt:
                    # An organic keyboard interrupt on mac or linux
                    self.stop()
                    raise
            if self._interrupted:
                # A hacky windows keyboard interrupt. stop() has already been
                # called.
                raise KeyboardInterrupt
        finally:
            self.router.close()
            self.router = None
            self.context = None
            self.port = self._initial_port
            self.running = False
            self.started.clear()
            self._interrupted = False
            self.stopping = False

    def _win32_keyboardinterrupt(self):
        self._interrupted = True
        self.stop()

    def _mainloop(self):
        while True:
            # Wait until we receive a request or a task is due:
            if self.tasks:
                timeout = max(0, 1000 * self.tasks.next().due_in())
            else:
                timeout = None
            events = self.poller.poll(timeout)
            if events:
                # A request was received:
                request = self.router.recv_multipart()
                if b'' not in request:
                    #  Malformed.
                    continue
                nroutes = request.index(b'')
                routing_id, msg = request[:nroutes], request[nroutes+1:]
                if not msg:  # pragma: no cover
                    # Malformed
                    continue
                command, args = msg[0], msg[1:]
                ip = self.router.peer_ip
                if command == b'log':
                    self.log(args)
                elif command == b'hello':
                    self.send(routing_id, b'hello')
                    logger.info("[%s] said hello", ip)
                elif command == b'protocol':
                    self.send(routing_id, PROTOCOL_VERSION.encode('utf8'))
                    logger.info("[%s] requested the protocol version", ip)
                elif command == b'check_access':
                    self.check_access(routing_id, ip, args)
                elif command == b'done':
                    self.done(routing_id, ip, args)
                elif command == b'stop' and self.stopping:
                    break
                else:
                    self.send(routing_id, ERR_INVALID_COMMAND)
                    logger.info("[%s] sent an invalid command", ip)
            else:
                # A task is due:
                task = self.tasks.pop()
                task()

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
        sock = self.context.socket(zmq.REQ, allow_insecure=self.allow_insecure)
        sock.connect('tcp://127.0.0.1:%d' % self.port)
        sock.send(b'stop')
        if self.run_thread is not None:
            self.run_thread.join()
        sock.close(linger=True)

    def send(self, routing_id, message):
        self.router.send_multipart(routing_id + [b'', message])


if __name__ == '__main__':
    port = 7340
    server = ZMQLogServer(
        port,
        handler_class=RotatingFileHandler,
        handler_kwargs={'maxBytes': 256, 'backupCount': 3},
    )
    server.run()
