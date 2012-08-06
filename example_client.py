from subproc_utils import setup_connection_with_parent
import threading

to_parent, from_parent = setup_connection_with_parent()

item = from_parent.get()
print 'client: got an item: '+ str(item)
to_parent.put(item)
to_parent.put(item)
to_parent.put(item)

import time
# To prove that this process gets killed when its parent ends:
time.sleep(1000)
