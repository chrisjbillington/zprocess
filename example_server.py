from subproc_utils import subprocess_with_queues, Event
import time

to_child, from_child, child = subprocess_with_queues('example_client.py')

# The normal kind of directly passing data to the child:
to_child.put(['<Some item!>','<some data!>'])
print 'server: got the item back: ', from_child.get()

# Posting an event that all processes in the tree can see (if there were more processes, of course):
foo_event = Event('foo',type='post')

for i in range(10):
    time.sleep(0.5)
    print 'server: posting a foo event with id=%d'%i
    foo_event.post(i, data='hello!')
