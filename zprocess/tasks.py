#####################################################################
#                                                                   #
# tasks.py                                                          #
#                                                                   #
# Copyright 2018, Chris Billington                                  #
#                                                                   #
# This file is part of the zprocess project (see                    #
# https://bitbucket.org/cbillington/zprocess) and is licensed under #
# the Simplified BSD License. See the license.txt file in the root  #
# of the project for the full license.                              #
#                                                                   #
#####################################################################
from __future__ import print_function, division, absolute_import, unicode_literals
try:
    from time import monotonic
except ImportError:
    from time import time as monotonic

from bisect import insort

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
    def __init__(self, due_in, func, *args, **kwargs):
        """Wrapper for a function call to be executed after a specified time interval.
        due_in is how long in the future, in seconds, the function should be called,
        func is the function to call. All subsequent arguments and keyword arguments
        will be passed to the function."""
        self.due_at = monotonic() + due_in
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.called = False

    def due_in(self):
        """The time interval in seconds until the task is due"""
        return self.due_at - monotonic()

    def __call__(self):
        if self.called:
            raise RuntimeError('Task has already been called')
        self.called = True
        return self.func(*self.args, **self.kwargs)

    def __gt__(self, other):
        # Tasks due sooner are 'greater than' tasks due later. This is necessary for
        # insort() and pop() as used with TaskQueue.
        return self.due_at < other.due_at


class TaskQueue(list):
    """A list of pending tasks due at certain times. Tasks are stored with the soonest
    due at the end of the list, to be removed with pop()"""

    def add(self, task):
        """Insert the task into the queue, maintaining sort order"""
        insort(self, task)

    def next(self):
        """Return the next due task, without removing it from the queue"""
        return self[-1]

    def cancel(self, task):
        self.remove(task)