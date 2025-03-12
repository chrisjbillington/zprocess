from zprocess import ProcessTree
import time

import os
this_folder = os.path.dirname(os.path.abspath(__file__))

process_tree = ProcessTree()
to_child, from_child, child = process_tree.subprocess(
    os.path.join(this_folder, 'example_child.py')
)

# The normal kind of directly passing data to the child:
to_child.put(['<Some item!>','<some data!>'])
print('parent: got the item back: ', from_child.get())

# Posting an event that all processes in the tree can see (if there were more processes,
# of course):
foo_event = process_tree.event('foo', role='post')

for i in range(10):
    time.sleep(0.5)
    print('parent: posting a foo event with id=%d'%i)
    foo_event.post(i, data='hello!')
