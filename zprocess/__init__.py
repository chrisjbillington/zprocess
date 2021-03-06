#####################################################################
#                                                                   #
# __init__.py                                                       #
#                                                                   #
# Copyright 2013, Chris Billington                                  #
#                                                                   #
# This file is part of the zprocess project (see                    #
# https://bitbucket.org/cbillington/zprocess) and is licensed under #
# the Simplified BSD License. See the license.txt file in the root  #
# of the project for the full license.                              #
#                                                                   #
#####################################################################

from __future__ import division, unicode_literals, print_function, absolute_import
import os
from pathlib import Path
import setuptools_scm

if 'COVERAGE_PROCESS_START' in os.environ:
    # We're running with coverage.py, likely running the test suite. Add
    # sigterm handler so that atexit handlers run even when terminated and
    # coverage data is saved:
    import signal
    def sigterm_handler(_signo, _stack_frame):
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, sigterm_handler)

import sys

PY2 = sys.version_info[0] == 2
if PY2:
    str = unicode
import zmq

if not zmq.zmq_version_info() >= (4, 3, 0):
    raise ImportError('zprocess requires libzmq >= 4.3')
    
_path, _cwd = os.path.split(os.getcwd())
if _cwd == 'zprocess' and _path not in sys.path:
    # Running from within zprocess dir? Add to sys.path for testing during
    # development:
    sys.path.insert(0, _path)

try:
    import importlib.metadata as importlib_metadata
except ImportError:
    import importlib_metadata

from .__version__ import __version__

# For communication between Python 2 and Python 3. Can be set by importing
# code to use a higher protocol in the case that it is known that both peers
# are a high enough version.
PICKLE_PROTOCOL = 2

# So that test code can suppress some output:
_silent = False



from zprocess.utils import (
    TimeoutError,
    Interrupted,
    Interruptor,
    start_daemon,
    embed,
    raise_exception_in_thread,
    disable_quick_edit,
)

from zprocess.clientserver import (
    ZMQServer,
    ZMQClient,
    zmq_get,
    zmq_get_multipart,
    zmq_get_string,
    zmq_get_raw,
    zmq_push,
    zmq_push_multipart,
    zmq_push_string,
    zmq_push_raw,
)

from zprocess.process_tree import (
    KillLock,
    Process,
    ProcessTree,
    setup_connection_with_parent,
    subprocess_with_queues,
    Event,
    RichStreamHandler,
    rich_print,
    ExternalBroker,
)




