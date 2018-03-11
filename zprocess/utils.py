from __future__ import division, unicode_literals, print_function, absolute_import
import sys
import threading
import zmq

PY2 = sys.version_info[0] == 2
if PY2:
    str = unicode


class TimeoutError(zmq.ZMQError):
    pass


def _reraise(exc_info):
    exctype, value, traceback = exc_info
    # handle python2/3 difference in raising exception        
    if PY2:
        exec('raise exctype, value, traceback', globals(), locals())
    else:
        raise value.with_traceback(traceback)


def raise_exception_in_thread(exc_info):
    """Raises an exception in a thread"""
    threading.Thread(target=_reraise, args=(exc_info,)).start()

