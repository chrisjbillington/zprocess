#####################################################################
#                                                                   #
# processclass_example.py                                           #
#                                                                   #
# Copyright 2024, Chris Billington                                  #
#                                                                   #
# This file is part of the zprocess project (see                    #
# https://bitbucket.org/cbillington/zprocess) and is licensed under #
# the Simplified BSD License. See the license.txt file in the root  #
# of the project for the full license.                              #
#                                                                   #
#####################################################################

from __future__ import print_function
from zprocess import Process
import os

class Foo(Process):
    def run(self, data):
        print('this is a running foo in process %d ' % os.getpid())
        print('data is', data)
        message = self.from_parent.get()
        print('foo, got a message:', message)
        self.to_parent.put('hello yourself!')
        import time
        time.sleep(1)
