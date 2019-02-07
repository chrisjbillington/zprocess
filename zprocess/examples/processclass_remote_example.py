#####################################################################
#                                                                   #
# processclass_example.py                                           #
#                                                                   #
# Copyright 2013, Chris Billington                                  #
#                                                                   #
# This file is part of the zprocess project (see                    #
# https://bitbucket.org/cbillington/zprocess) and is licensed under #
# the Simplified BSD License. See the license.txt file in the root  #
# of the project for the full license.                              #
#                                                                   #
#####################################################################

from __future__ import print_function
from zprocess import Process, ProcessTree
import os

"""Before running this example program, you need to start a remote process server.
Start one in a terminal with:

    python -m zprocess.remote -tui

then run this script.

To test across multiple machines, set REMOTE_HOST below to the hostname of the other
computer, and generate a shared secret and save it to file with:

    python -m zprocess.makesecret <filename>

Set the filename below as SHARED_SECRET_FILE and copy it (securely) to the other
computer. Then run on the other computer:

python -m zprocess.remote --shared-secret <filename>

to start a remote process server using the same shared secret, allowing us to
communicate with it securely.
"""

REMOTE_HOST = 'localhost'
SHARED_SECRET_FILE = None


if SHARED_SECRET_FILE is not None:
    with open(SHARED_SECRET_FILE) as f:
        shared_secret = open(SHARED_SECRET_FILE).read().strip()
else:
    shared_secret = None


class Foo(Process):
    def run(self, data):
        print('this is a running foo in process %d' % os.getpid())
        print('data is', data)
        message = self.from_parent.get()
        print('foo, got a message:', message)
        self.to_parent.put('hello yourself!')
        import time
        time.sleep(1)


# This __main__ check is important to stop the same code executing again in the child:
if __name__ == '__main__':
    print("Note: script wil not work without starting a zprocess.remote server.")
    print("See comments in the script for instructions")
    process_tree = ProcessTree(shared_secret)
    remote_process_client = process_tree.remote_process_client(REMOTE_HOST)
    foo = Foo(process_tree, remote_process_client=remote_process_client)
    to_child, from_child = foo.start('bar')
    to_child.put('hello, foo!')
    response = from_child.get()
    print('parent, got a response:', response)
    foo.terminate()
    print('here')