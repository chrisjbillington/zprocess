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

from zprocess import Process
import os

class Foo(Process):
    def run(self, data):
        print 'this is a running foo in process', os.getpid()
        print 'data is', data
        message = self.from_parent.get()
        print 'foo, got a message:', message
        self.to_parent.put('hello yourself!')

# This __main__ check is important to stop the same code executing again in the child:
if __name__ == '__main__':
    foo = Foo()
    to_child, from_child = foo.start('bar')
    to_child.put('hello, foo!')
    response = from_child.get()
    print 'parent, got a response:', response
