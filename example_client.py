from subproc_utils import setup_connection_with_parent, Event

to_parent, from_parent = setup_connection_with_parent()

# The normal kind of getting data from the parent directly:
item = from_parent.get()
print 'client: got an item: '+ str(item)
print 'client: sending the item back...'
to_parent.put(item)

# Waiting for an event posted by the parent (though it could be posted by any process, we don't care):
foo_event = Event('foo',type='wait')
for i in range(10):
    data = foo_event.wait(i, timeout=1)
    print 'client: received foo event %d. Data was: %s'%(i, str(data))
    
import time
# To prove that this process gets killed when its parent ends:
time.sleep(1000)
