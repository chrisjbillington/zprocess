from time import monotonic
from bisect import insort
import copy

# This module contains a minimalistic task queue. It is used by the zlock and zlog
# servers (zprocess.locking and zprocess.logging respectively) in order to wait for
# requests from clients for a limited amount of time so that other due tasks can be
# performed at set times. This is similar to the kind of functionality provided by
# async/await, but is simpler and more performant. I may make the jump to asyncio in the
# future for this functionality, but this serves the needs of these two servers well for
# the moment.
#
# Usage is to instantiate a TaskQueue, instantiate Task objects, and then call
# TaskQueue.add() to add them to the queue. One can then inspect how long until the next
# task is due with TaskQueue.next().due_in(), and use that number as a timeout for any
# blocking operation to be performed. The next task then then be obtained (and removed
# from the queue) with:
#
# task = TaskQueue.pop()
#
# and run by calling it:
#
# task()


class Task(object):
    _next_id = 0
    def __init__(self, due_in, func, *args, **kwargs):
        """Wrapper for a function call to be executed after a specified time interval.
        `due_in` is how long in the future, in seconds, the function should be called,
        func is the function to call. All subsequent arguments and keyword arguments
        will be passed to the function. If added to a TaskQueue with `repeat=True`, or
        if Task.repeat is set to True, then the `due_in` argument is used as the
        repetition interval."""
        self.due_at = monotonic() + due_in
        self.interval = due_in
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self._called = False
        self.repeat = False
        self._id = self._next_id
        Task._next_id += 1

    def due_in(self):
        """The time interval in seconds until the task is due"""
        return self.due_at - monotonic()

    def __call__(self):
        if self._called:
            raise RuntimeError('Task has already been called')
        self._called = True
        return self.func(*self.args, **self.kwargs)

    def __gt__(self, other):
        # Tasks due sooner are 'greater than' tasks due later. This is necessary for
        # insort() and pop() as used with TaskQueue.
        return self.due_at < other.due_at

    def __eq__(self, other):
        return isinstance(other, Task) and self._id == other._id


class TaskQueue(list):
    """A list of pending tasks due at certain times. Tasks are stored with the soonest
    due at the end of the list, to be removed with pop()"""

    def pop(self):
        """Returns the next due task. If Task.repeat is True, re-adds a copy of the Task
        to the queue with a due time `Task.interval` after its previous due time."""
        task = super().pop()
        if task.repeat:
            next_rep = copy.copy(task)
            # If next due time would be in the past, add a task due now instead. We
            # don't want to a large number of tasks if e.g. system suspension led to a
            # discontinuous jump forward in the monotonic clock
            next_rep.due_at = max(monotonic(), task.due_at + task.interval)
            self.add(next_rep)
        return task

    def add(self, task, repeat=None):
        """Insert the task into the queue, maintaining sort order. If repeat is given,
        this sets `Task.repeat`, which controls whether or not a copy of the task will
        be re-added to the queue after it is removed via a call to `TaskQueue.pop()`."""
        if repeat is not None:
            task.repeat = repeat
        # Re-adding an already-called task resets it
        task._called = False
        insort(self, task)

    def next(self):
        """Return the next due task, without removing it from the queue"""
        return self[-1]

    def cancel(self, task):
        self.remove(task)


if __name__ == '__main__':
    # Intended use. It's up the caller to sleep() or otherwise wait until the next task
    # is due. It is assumed that the queue is only accessed from a single thread such
    # that it's not possible for any tasks to be added to the queue during such a sleep.
    # More usefully, instead of calling sleep(), an application might be calling
    # select.select() or select.poll() with the time to the next task as the timeout.
    # This way one can implement a simple mainloop for an event-driven application that
    # can respond to network and file events as well as executing time-based tasks. This
    # is how the mainloops of the zlock and zlog servers are implemented.
    import time

    tasks = TaskQueue()
    task = Task(1, print, 'hello', 'world')

    tasks.add(task, repeat=True)
    while tasks:
        time.sleep(max(0, tasks.next().due_in()))  # don't pass sleep() a negative value
        next_task = tasks.pop()
        next_task()
