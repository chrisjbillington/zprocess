import os
import sys
import subprocess
import threading
import zmq

class ReadQueue(object):
    def __init__(self,sock):
        self.sock = sock
        self.lock = threading.Lock()
    def get(self):
        with self.lock:
            obj = self.sock.recv_pyobj()
        return obj
        
class WriteQueue(object):
    def __init__(self,sock):
        self.sock = sock
        self.lock = threading.Lock()
    def put(self,obj):
        with self.lock:
            self.sock.send_pyobj(obj)

def kill_on_exit(process):
    """Make a process quit when the process calling this function does"""
    path = os.path.join(os.path.dirname(__file__),'killswitch.py')
    killswitch = subprocess.Popen(['python',path,str(process.pid), str(os.getpid())])
    
def subprocess_with_queues(path):
    context = zmq.Context()
    
    to_child = context.socket(zmq.PUSH)
    from_child = context.socket(zmq.PULL)
    
    port_from_child = from_child.bind_to_random_port('tcp://127.0.0.1')
    
    child = subprocess.Popen(['python', '-u', path, str(port_from_child)])
    
    port_to_child = from_child.recv()
    to_child.connect('tcp://127.0.0.1:%s'%port_to_child)
    
    to_child = WriteQueue(to_child)
    from_child = ReadQueue(from_child)
    
    kill_on_exit(child)
    
    return to_child, from_child, child
    
def setup_connection_with_parent():
    port_to_parent = int(sys.argv[1])

    context = zmq.Context()
    
    to_parent = context.socket(zmq.PUSH)
    from_parent = context.socket(zmq.PULL)
    
    port_from_parent = from_parent.bind_to_random_port('tcp://127.0.0.1')
    
    to_parent.connect("tcp://127.0.0.1:%s"%port_to_parent)
    to_parent.send(str(port_from_parent))
    
    from_parent = ReadQueue(from_parent)
    to_parent = WriteQueue(to_parent)
    return to_parent, from_parent
    
