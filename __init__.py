import os
import sys
import subprocess
import threading
import zmq
    
if os.name == 'nt':
    python = 'pythonw'
else:
    python = 'python'
    
class WriteQueue(object):
    """Provides writing of python objects to the underlying zmq socket,
    with added locking. No reading is supported, once you put an object,
    you can't check what was put or whether the items have been gotten"""
    def __init__(self,sock):
        self.sock = sock
        self.lock = threading.Lock()
    def put(self,obj):
        with self.lock:
            self.sock.send_pyobj(obj)

class ReadQueue(object):
    """provides reading and writing methods to the underlying zqm socket,
    with added locking. Actually there are two sockets, one for reading,
    one for writing. The only real use case for writing is when the
    read socket is blocking, but the process at the other end has died,
    and you need to stop the thread that is blocking on the read. So
    you send it a quit signal with put()."""
    def __init__(self,sock,to_self_sock):
        self.sock = sock
        self.to_self_sock = to_self_sock
        self.socklock = threading.Lock()
        self.to_self_sock_lock = threading.Lock()
    def get(self):
        with self.socklock:
            obj = self.sock.recv_pyobj()
        return obj
        
    def put(self, obj):
        with self.to_self_sock_lock:
            self.to_self_sock.send_pyobj(obj)
        
        
def kill_on_exit(process):
    """Make a process quit when the process calling this function does"""
    path = os.path.join(os.path.dirname(__file__),'killswitch.py')
    killswitch = subprocess.Popen([python,path,str(process.pid), str(os.getpid())])
    
def subprocess_with_queues(path):
    context = zmq.Context()

    to_child = context.socket(zmq.PUSH)
    from_child = context.socket(zmq.PULL)
    to_self = context.socket(zmq.PUSH)
    
    port_from_child = from_child.bind_to_random_port('tcp://127.0.0.1')
    to_self.connect('tcp://127.0.0.1:%s'%port_from_child)
    
    child = subprocess.Popen([python, '-u', path, str(port_from_child)])
    
    port_to_child = from_child.recv()
    to_child.connect('tcp://127.0.0.1:%s'%port_to_child)
    
    to_child = WriteQueue(to_child)
    from_child = ReadQueue(from_child, to_self)
    
    kill_on_exit(child)
    
    return to_child, from_child, child
    
def setup_connection_with_parent():
    port_to_parent = int(sys.argv[1])

    context = zmq.Context()
    
    to_parent = context.socket(zmq.PUSH)
    from_parent = context.socket(zmq.PULL)
    to_self = context.socket(zmq.PUSH)
        
    port_from_parent = from_parent.bind_to_random_port('tcp://127.0.0.1')
    to_self.connect('tcp://127.0.0.1:%s'%port_from_parent)
    
    to_parent.connect("tcp://127.0.0.1:%s"%port_to_parent)
    to_parent.send(str(port_from_parent))
    
    from_parent = ReadQueue(from_parent, to_self)
    to_parent = WriteQueue(to_parent)
    return to_parent, from_parent
    
