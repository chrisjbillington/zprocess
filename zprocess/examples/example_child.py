#####################################################################
#                                                                   #
# example_child.py                                                 #
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
from zprocess import ProcessTree

process_tree = ProcessTree.connect_to_parent()
to_parent = process_tree.to_parent
from_parent = process_tree.from_parent

# The normal kind of getting data from the parent directly:
item = from_parent.get()
print('child: got an item: '+ str(item))
print('child: sending the item back...')
to_parent.put(item)

# Waiting for an event posted by the parent (though it could be posted by any process,
# we don't care):
foo_event = process_tree.event('foo', role='wait')
for i in range(10):
    data = foo_event.wait(i, timeout=1)
    print('child: received foo event %d. Data was: %s' % (i, str(data)))
    
import time
# To prove that this process gets killed when its parent ends:
time.sleep(1000)
