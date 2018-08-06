from __future__ import print_function, division, absolute_import, unicode_literals
import time
import zmq
from bisect import insort
from collections import deque
import threading

MAX_RESPONSE_TIME = 1  # second
MAX_ABSENT_TIME = 1  # second


class AlreadyHeld(ValueError):
    pass


class AlreadyWaiting(ValueError):
    pass


class NotHeld(ValueError):
    pass


class ConcurrentRequests(ValueError):
    pass


class Task(object):
    def __init__(self, due_in, func, *args, **kwargs):
        """Wrapper for a function call to be executed after a specified time interval.
        due_in is how long in the future, in seconds, the function should be called,
        func is the function to call. All subsequent arguments and keyword arguments
        will be passed to the function."""
        self.due_at = time.time() + due_in
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def due_in(self):
        """The time interval in seconds until the task is due"""
        return self.due_at - time.time()

    def __call__(self):
        return self.func(*self.args, **self.kwargs)

    def __gt__(self, other):
        # Ordering of tasks is defined by their due time
        return self.due_at > other.due_at


class TaskQueue(object):
    def __init__(self):
        """Class representing a list of pending tasks due at certain times."""
        self.queue = deque()

    def add(self, task):
        """Insert the task into the queue, maintaining sort order"""
        insort(self.queue, task)

    def pop(self):
        """Return the next due task, removing it from the queue"""
        return self.queue.popleft()

    def next(self):
        """Return the next due task, without removing it from the queue"""
        return self.queue[0]

    def cancel(self, task):
        """Remove a task from the queue"""
        self.queue.remove(task)

    def __bool__(self):
        # Whether there are any tasks in the queue
        return bool(self.queue)


class Lock(object):
    """Object to represent a readers-writer lock. Implementation gives priority to
    writers"""

    def __init__(self, key, active_locks):
        self.key = key
        # Dict of active locks from the parent ZMQLockServer
        self.active_locks = active_locks
        self.waiting_readers = set()
        self.waiting_writers = set()
        self.readers = set()
        self.writer = None

    def _check_cleanup(self):
        """Delete the instance from the ZMQServer's dict of active locks if there are no
        readers, writers or waiters"""
        if not any((self.readers, self.waiting_readers, self.waiting_writers)):
            if self.writer is None:
                del self.active_locks[self.key]

    def acquire(self, client_id, read_only):
        """Attempt to acquire the lock for the given client. Return True on success, or
        False upon failure. In the latter case, the client will be added to an internal
        list of clients that are waiting for the lock"""
        try:
            if client_id in self.readers or client_id == self.writer:
                raise AlreadyHeld
            if read_only:
                if client_id in self.waiting_readers:
                    raise AlreadyWaiting
                # The reader can have the lock if there is no writer or waiting writers:
                if self.writer is None and not self.waiting_writers:
                    self.readers.add(client_id)
                    return True
                else:
                    self.waiting_readers.add(client_id)
                    return False
            else:
                if client_id in self.waiting_writers:
                    raise AlreadyWaiting
                # The writer can have the lock if there are no other readers or writers:
                if self.writer is None and not self.readers:
                    self.writer = client_id
                    return True
                else:
                    self.waiting_writers.add(client_id)
                    return False
        finally:
            self._check_cleanup()

    def release(self, client_id):
        """Release the lock held by the given client. If this makes the lock available
        for other waiting clients, acquire the lock for those clients. Return a set of
        client ids that acquired the lock in this way."""
        try:
            if client_id in self.readers:
                self.readers.remove(client_id)
                # Is the lock now available for a writer?
                if self.waiting_writers and not self.readers:
                    self.writer = self.waiting_writers.pop()
                    return {self.writer}
            elif client_id == self.writer:
                self.writer = None
                # Is there a waiting writer to give the lock to?
                if self.waiting_writers:
                    self.writer = self.waiting_writers.pop()
                    return {self.writer}
                # Are there waiting readers to give the lock to?
                if self.waiting_readers:
                    self.readers = set(self.waiting_readers)
                    acquired = self.waiting_readers
                    self.waiting_readers = set()
                    return acquired
            else:
                raise NotHeld
            return set()
        finally:
            self._check_cleanup()

    def give_up(self, client_id):
        """Remove the client from the list of waiting clients"""
        if client_id in self.waiting_readers:
            self.waiting_readers.remove(client_id)
        elif client_id in self.waiting_writers:
            self.waiting_writers.remove(client_id)
        self._check_cleanup()


class ZMQLockServer(object):
    def __init__(self, port=None, bind_address='tcp://0.0.0.0'):
        self.port = port
        self._initial_port = port
        self.bind_address = bind_address
        self.context = None
        self.router = None
        self.tasks = TaskQueue()
        self.active_locks = {}

        # Lock-acquiring clients we haven't replied to yet:
        self.pending_requests = {}
        # Tasks for timing out locks if clients don't release them:
        self.timeout_tasks = {}
        # Tasks for advising the clients that they need to retry:
        self.advise_retry_tasks = {}
        # Tasks for giving up trying to acquire a lock if the client does not retry:
        self.give_up_tasks = {}
        # Tasks for releasing a lock that was acquired after a client was advised that
        # it needed to retry, but before it responded with a retry request.
        self.absentee_release_tasks = {}

        self.run_thread = None
        self.stopping = False
        self.started = threading.Event()

    def get_lock(self, key):
        """Return the lock object with the given key if it already exists, else make
        a new instance"""
        if key in self.active_locks:
            return self.active_locks[key]
        else:
            lock = Lock(key, self.active_locks)
            self.active_locks[key] = lock
            return lock

    def run(self):
        self.context = zmq.Context.instance()
        self.router = self.context.socket(zmq.ROUTER)
        if self.port is not None:
            self.router.bind('%s:%d' % (self.bind_address, self.port))
        else:
            self.port = self.router.bind_to_random_port(self.bind_address)
        self.started.set()
        print('starting')
        while True:
            # Wait until we receive a request or a task is due:
            if self.tasks:
                timeout = max(0, 1000 * self.tasks.next().due_in())
            else:
                timeout = None
            events = self.router.poll(timeout, flags=zmq.POLLIN)
            if events:
                # A request was received:
                request = self.router.recv_multipart()
                print('received:', request)
                if len(request) < 3 or request[1] != b'':
                    # Not well formed as [routing_id, '', command, ...]
                    print('not well formed')
                    continue
                routing_id, command, args = request[0], request[2], request[3:]
                if command == b'hello':
                    self.send(routing_id, b'hello')
                elif command == b'acquire':
                    self.acquire_request(routing_id, args)
                elif command == b'release':
                    self.release_request(routing_id, args)
                elif command == b'stop' and self.stopping:
                    self.send(routing_id, b'ok')
                    break
                else:
                    self.send(routing_id, b'error: invalid command')
            else:
                # A task is due:
                task = self.tasks.pop()
                task()
        self.router.close()
        self.router = None
        self.context = None
        self.port = self._initial_port
        self.started.clear()

    def run_in_thread(self):
        """Run the main loop in a separate thread, returning immediately"""
        self.run_thread = threading.Thread(target=self.run)
        self.run_thread.daemon = True
        self.run_thread.start()
        self.started.wait()

    def stop(self):
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
        print('sending:', [routing_id, b'', message])
        self.router.send_multipart([routing_id, b'', message])

    def start_request(self, routing_id, client_id, timeout, read_only):
        """Record that we have a pending request for a certain client, so that
        we can return errors if a client with the same client_id sends us
        more requests before we have responded to the first one."""
        if client_id in self.pending_requests:
            msg = b'error: multiple concurrent requests with same client_id'
            self.send(routing_id, msg)
            raise ConcurrentRequests
        else:
            self.pending_requests[client_id] = routing_id, timeout, read_only

    def end_request(self, client_id):
        """Mark a pending request as done"""
        del self.pending_requests[client_id]

    def acquire_request(self, routing_id, args):
        if not 3 <= len(args) <= 4:
            self.send(routing_id, b'error: wrong number of arguments')
            return
        key, client_id, timeout = args[:3]
        try:
            timeout = float(timeout)
        except ValueError:
            self.send(routing_id, b'error: timeout %s not a number' % timeout)
            return
        if len(args) == 4:
            if args[3] != b'read_only':
                self.send(routing_id, b"error: expected 'read_only', got %s" % args[3])
                return
            read_only = True
        else:
            read_only = False
        try:
            self.start_request(routing_id, client_id, timeout, read_only)
        except ConcurrentRequests:
            return
        else:
            self.acquire(routing_id, key, client_id, timeout, read_only)

    def release_request(self, routing_id, args):
        if not len(args) == 2:
            self.send(routing_id, b'error: wrong number of arguments')
        key, client_id = args
        self.release(routing_id, key, client_id)

    def acquire(self, routing_id, key, client_id, timeout, read_only):
        """Acquire a lock for a client."""
        if (key, client_id, read_only) in self.absentee_release_tasks:
            # Lock was previously acquired in the client's absence:
            self.cancel_absentee_release(key, client_id, read_only)
            self.schedule_timeout(key, client_id, timeout)
            self.send(routing_id, b'ok')
            self.end_request(client_id)
            return
        elif (key, client_id, not read_only) in self.absentee_release_tasks:
            # Lock was previously acquired in the client's absence, but with a
            # different value for read_only. Release it and process as normal:
            self.cancel_absentee_release(key, client_id, not read_only)
            self.release(None, key, client_id)

        lock = self.get_lock(key)

        if (key, client_id) in self.give_up_tasks:
            # This is a retry. Do not give_up, for the client has returned.
            self.cancel_give_up(key, client_id)
            # Keep waiting until the lock is free:
            self.schedule_advise_retry(routing_id, key, client_id)
        else:
            try:
                success = lock.acquire(client_id, read_only)
            except AlreadyHeld:
                self.send(routing_id, b'error: lock already held')
                self.end_request(client_id)
            else:
                if success:
                    self.post_acquire(routing_id, key, client_id, timeout, read_only)
                else:
                    self.schedule_advise_retry(routing_id, key, client_id)

    def post_acquire(self, routing_id, key, client_id, timeout, read_only):
        """Actions to be taken after a lock was acquired. If routing_id is not None,
        then the acquisition was whilst a client was waiting for a response, in which
        case we send the response. Otherwise the client is absentee and we schedule the
        lock to be released after MAX_ABSENT_TIME if the client does not retry"""
        if routing_id is None:
            self.schedule_absentee_release(key, client_id, read_only)
        else:
            self.send(routing_id, b'ok')
            self.end_request(client_id)
            self.schedule_timeout(key, client_id, timeout)
            if (routing_id, key, client_id) in self.advise_retry_tasks:
                self.cancel_advise_retry(routing_id, key, client_id)

    def release(self, routing_id, key, client_id):
        """Release a lock for a client. If routing_id is None, then it is assumed that
        there is no pending request to reply to."""
        for k in ((key, client_id, True), (key, client_id, False)):
            # If the lock was acquired while the client was absent, then it has not
            # followed protocol in acquiring the lock, so deny it:
            if k in self.absentee_release_tasks:
                self.send(routing_id, b'error: lock not held')
                self.cancel_absentee_release(*k)
                self.absentee_release(*k)
                return

        lock = self.get_lock(key)
        try:
            acquirers = lock.release(client_id)
        except NotHeld:
            assert routing_id is not None
            self.send(routing_id, b'error: lock not held')
        else:
            if routing_id is not None:
                self.send(routing_id, b'ok')
                self.cancel_timeout(key, client_id)
            self.process_triggered_acquisitions(key, acquirers)

    def process_triggered_acquisitions(self, key, acquirers):
        """When the lock was acquired by a number of clients as a result of being
        released, call self.post_acquire on each one"""
        for client_id in acquirers:
            routing_id, timeout, read_only = self.pending_requests[client_id]
            self.post_acquire(routing_id, key, client_id, timeout, read_only)

    def schedule_advise_retry(self, routing_id, key, client_id):
        """Queue up a task to tell the client to retry if we have not acquired the lock
        for it after MAX_RESPONSE_TIME"""
        task = Task(MAX_RESPONSE_TIME, self.advise_retry, routing_id, key, client_id)
        self.tasks.add(task)
        self.advise_retry_tasks[routing_id, key, client_id] = task

    def cancel_advise_retry(self, routing_id, key, client_id):
        """Cancel a scheduled advise_retry task"""
        task = self.advise_retry_tasks.pop((routing_id, key, client_id))
        self.tasks.cancel(task)

    def advise_retry(self, routing_id, key, client_id):
        """Tell the client to retry acquiring the lock"""
        self.send(routing_id, b'retry')
        self.end_request(client_id)
        del self.advise_retry_tasks[routing_id, key, client_id]
        self.schedule_give_up(key, client_id)

    def schedule_timeout(self, key, client_id, timeout):
        """Queue up a task to release the lock if the client has not released it
        before the timeout"""
        task = Task(timeout, self.timeout, key, client_id)
        self.tasks.add(task)
        self.timeout_tasks[key, client_id] = task

    def cancel_timeout(self, key, client_id):
        """Cancel a scheduled timeout task"""
        task = self.timeout_tasks.pop((key, client_id))
        self.tasks.cancel(task)

    def timeout(self, key, client_id):
        """Release a lock that has been held for too long"""
        self.release(None, key, client_id)
        del self.timeout_tasks[key, client_id]

    def schedule_absentee_release(self, key, client_id, read_only):
        """Queue up a task to release the lock, which was acquired in the client's
        absence, if it does not sent a retry request within MAX_ABSENT_TIME"""
        task = Task(MAX_ABSENT_TIME, self.absentee_release, key, client_id, read_only)
        self.tasks.add(task)
        self.absentee_release_tasks[key, client_id, read_only] = task

    def cancel_absentee_release(self, key, client_id, read_only):
        """Cancel a scheduled absentee_release task"""
        task = self.absentee_release_tasks.pop((key, client_id, read_only))
        self.tasks.cancel(task)

    def absentee_release(self, key, client_id, read_only):
        """Release a lock that was acquired in the client's absence, if the client did
        not send a retry request within MAX_ABSENT_TIME"""
        self.release(None, key, client_id)
        del self.absentee_release_tasks[key, client_id, read_only]

    def schedule_give_up(self, key, client_id):
        """Queue up a task to give up waiting for the lock if the client doesn't retry
        within MAX_ABSENT_TIME"""
        task = Task(MAX_ABSENT_TIME, self.give_up, key, client_id)
        self.tasks.add(task)
        self.give_up_tasks[key, client_id] = task

    def cancel_give_up(self, key, client_id):
        """Cancel a scheduled give_up task"""
        task = self.give_up_tasks.pop((key, client_id))
        self.tasks.cancel(task)

    def give_up(self, key, client_id):
        """Give up waiting for a lock, if a client did not retry within
        MAX_ABSENT_TIME"""
        lock = self.get_lock(key)
        lock.give_up(client_id)
        del self.give_up_tasks[key, client_id]


if __name__ == '__main__':
    port = 7339
    server = ZMQLockServer(port)
    server.run()
