from subproc_utils import subprocess_with_queues

to_child, from_child, child = subprocess_with_queues('example_client.py')
to_child.put(['<Some item!>','<some data!>'])
print 'server: got the item back: ', from_child.get()
