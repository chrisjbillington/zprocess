import os
import sys
import subprocess
import threading
import zmq
import time
import signal
from socket import gethostbyname

context = zmq.Context.instance()

def raise_exception_in_thread(exc_info):
    """Raises an exception in a thread"""
    def f(exc_info):
        raise exc_info[0], exc_info[1], exc_info[2]
    threading.Thread(target=f,args=(exc_info,)).start()

class ZMQServer(object):
    def __init__(self,port):
        self.sock = context.socket(zmq.REP)
        self.sock.bind('tcp://0.0.0.0:%s'%str(port))
        self.mainloop_thread = threading.Thread(target=self.mainloop)
        self.mainloop_thread.daemon = True
        self.mainloop_thread.start()
        
    def mainloop(self):
        while True:
            request_data = self.sock.recv_pyobj()
            try:
                response_data = self.handler(request_data)
            except Exception as e:
                # Raise the exception in a separate thread so that the
                # server keeps running:
                exc_info = sys.exc_info()
                raise_exception_in_thread(exc_info)
                response_data = zmq.ZMQError('The server had an unhandled exception whilst processing the request: %s'%str(e))
            self.sock.send_pyobj(response_data)
            
            
    def handler(self, request_data):
        """To be overridden by subclasses. This is an example
        implementation"""
        response = 'This is an example ZMQServer. Your request was %s.'%str(request_data)
        return response
        
class ZMQGet(object):
    def __init__(self):
        self.sock = None
        self.poller = None
        self.host = None
        self.port = None
        self.lock = threading.Lock()
        
    def __call__(self, port, host='localhost', data=None, timeout=5):
        # zmq sockets are not threadsafe, so serialise all calls to this function:
        with self.lock:
            host = gethostbyname(host)
            # We cache the socket so as to not exhaust ourselves of tcp
            # ports. However if a different server is in use, we need a new
            # socket. Also if we don't have a socket, we also need a new one:
            if self.sock is None or host != self.host or port != self.port:
                self.host = host
                self.sock = context.socket(zmq.REQ)
                self.sock.setsockopt(zmq.LINGER, 0)
                self.poller = zmq.Poller()
                self.poller.register(self.sock, zmq.POLLIN)
                self.sock.connect('tcp://%s:%s'%(host, str(port)))
            self.sock.send_pyobj(data, zmq.NOBLOCK)
            events = self.poller.poll(timeout*1000) # convert timeout to ms
            if events:
                response = self.sock.recv_pyobj()
            else:
                # The server hasn't replied. We don't know what it's doing,
                # so we'd better stop using this socket in case late messages
                # arrive on it in the future:
                self.sock = None
                raise zmq.ZMQError('No response from server: timed out')
            if isinstance(response, Exception):
                raise response
            else:
                return response

# Actually need to instantiate it!        
zmq_get = ZMQGet()

class HeartbeatServer(object):
    """A server which recieves messages from clients and echoes them
    back. There is only one server for however many clients there are"""
    def __init__(self):
        self.running = False
        
    def run(self):
        if not self.running:
            self.running = True
            self.sock = context.socket(zmq.REP)
            self.port = self.sock.bind_to_random_port('tcp://127.0.0.1')
            self.mainloop_thread = threading.Thread(target=self.mainloop)
            self.mainloop_thread.daemon = True
            self.mainloop_thread.start()
        return self.port
        
    def mainloop(self):
        zmq.device(zmq.FORWARDER, self.sock, self.sock)

class HeartbeatClient(object):
    def __init__(self, server_port, lock):
        self.sock = context.socket(zmq.REQ)
        self.sock.setsockopt(zmq.LINGER, 0)
        self.poller = zmq.Poller()
        self.poller.register(self.sock,zmq.POLLIN)
        self.port = self.sock.connect('tcp://127.0.0.1:%s'%server_port)
        self.mainloop_thread = threading.Thread(target=self.mainloop)
        self.mainloop_thread.daemon = True
        self.mainloop_thread.start()
        self.pid = str(os.getpid())
        self.lock = lock

    def mainloop(self):
        while True:
            try:
                time.sleep(1)
                self.sock.send(self.pid, zmq.NOBLOCK)
                events = self.poller.poll(1000)
                if events:
                    msg = self.sock.recv()
                    assert msg == self.pid
                else:
                    break
            except Exception as e:
                break
        if self.lock is not None:
            with self.lock: 
                os.kill(os.getpid(), signal.SIGTERM)
        else:
            os.kill(os.getpid(), signal.SIGTERM)
    
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

class OutputInterceptor(object):

    def __init__(self, queue, streamname='stdout'):
        self.queue = queue
        self.streamname = streamname
        self.real_stream = getattr(sys,streamname)
        self.fileno = self.real_stream.fileno
        
    def connect(self):
        setattr(sys,self.streamname,self)
    
    def disconnect(self):
        setattr(sys,self.streamname,self.real_stream)
            
    def write(self, s):
        self.queue.put([self.streamname, s])
        
    def close(self):
        self.disconnect()
        self.real_stream.close()
        
    def flush(self):
        pass
        
            
def subprocess_with_queues(path):

    to_child = context.socket(zmq.PUSH)
    from_child = context.socket(zmq.PULL)
    to_self = context.socket(zmq.PUSH)
    
    port_from_child = from_child.bind_to_random_port('tcp://127.0.0.1')
    to_self.connect('tcp://127.0.0.1:%s'%port_from_child)
    
    heartbeat_port = heartbeat_server.run()
    
    child = subprocess.Popen([sys.executable, '-u', path, str(port_from_child),str(heartbeat_port)])
    
    port_to_child = from_child.recv()
    to_child.connect('tcp://127.0.0.1:%s'%port_to_child)
    
    to_child = WriteQueue(to_child)
    from_child = ReadQueue(from_child, to_self)
    
    return to_child, from_child, child
    
def setup_connection_with_parent(lock=False,redirect_output=False):
    port_to_parent = int(sys.argv[1])
    port_to_heartbeat_server = int(sys.argv[2])

    to_parent = context.socket(zmq.PUSH)
    from_parent = context.socket(zmq.PULL)
    to_self = context.socket(zmq.PUSH)
        
    port_from_parent = from_parent.bind_to_random_port('tcp://127.0.0.1')
    to_self.connect('tcp://127.0.0.1:%s'%port_from_parent)
    
    to_parent.connect("tcp://127.0.0.1:%s"%port_to_parent)
    to_parent.send(str(port_from_parent))
    
    from_parent = ReadQueue(from_parent, to_self)
    to_parent = WriteQueue(to_parent)
    
    if redirect_output:
        stdout = OutputInterceptor(to_parent)
        stderr = OutputInterceptor(to_parent,'stderr')
        stdout.connect()
        stderr.connect()
    
    global heartbeat_client
    kill_lock = threading.Lock()
    heartbeat_cient = HeartbeatClient(port_to_heartbeat_server,kill_lock) 
    if lock:
        return to_parent, from_parent, kill_lock
    else:
        return to_parent, from_parent

heartbeat_server = HeartbeatServer()
