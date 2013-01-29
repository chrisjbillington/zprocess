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
        self.port = port
        self.sock = context.socket(zmq.REP)
        self.sock.setsockopt(zmq.LINGER, 0)
        self.sock.bind('tcp://0.0.0.0:%s'%str(self.port))
        self.mainloop_thread = threading.Thread(target=self.mainloop)
        self.mainloop_thread.daemon = True
        self.shutting_down = False
        self.finished_shutting_down = threading.Event()
        self.mainloop_thread.start()
        
    def mainloop(self):
        while True:
            request_data = self.sock.recv_pyobj()
            if request_data == 'shutdown' and self.shutting_down:
                self.sock.close(linger=False)
                self.finished_shutting_down.set()
                break
            try:
                response_data = self.handler(request_data)
            except Exception as e:
                # Raise the exception in a separate thread so that the
                # server keeps running:
                exc_info = sys.exc_info()
                raise_exception_in_thread(exc_info)
                response_data = zmq.ZMQError('The server had an unhandled exception whilst processing the request: %s'%str(e))
            self.sock.send_pyobj(response_data)
            
    def shutdown(self):
        self.shutting_down = True
        sock = context.socket(zmq.REQ)
        sock.connect('tcp://0.0.0.0:%s'%str(self.port))
        sock.send_pyobj('shutdown')
        self.finished_shutting_down.wait()
            
    def handler(self, request_data):
        """To be overridden by subclasses. This is an example
        implementation"""
        response = 'This is an example ZMQServer. Your request was %s.'%str(request_data)
        return response
        
        
class ZMQGet(object):
    def __init__(self,type='pyobj'):
        self.local = threading.local()
        self.type = type
        
    def new_socket(self,host,port):
        # Every time the REQ/REP cadence is broken, we need to create
        # and bind a new socket to get it back on track. Also, we have
        # a separate socket for each thread. Also a new socket if there
        # is a different host or port:
        self.local.host = gethostbyname(host)
        self.local.port = int(port)
        context = zmq.Context.instance()
        self.local.sock = context.socket(zmq.REQ)
        self.local.sock.setsockopt(zmq.LINGER, 0)
        self.local.poller = zmq.Poller()
        self.local.poller.register(self.local.sock, zmq.POLLIN)
        self.local.sock.connect('tcp://%s:%d'%(self.local.host, self.local.port))
        # Different send/recv methods depending on the desired protocol:
        if self.type == 'pyobj':
            self.local.send = self.local.sock.send_pyobj
            self.local.recv = self.local.sock.recv_pyobj
        elif self.type == 'multipart':
            self.local.send = self.local.sock.send_multipart
            self.local.recv = self.local.sock.recv_multipart
        elif self.type == 'raw':
            self.local.send = self.local.sock.send
            self.local.recv = self.local.sock.recv
            
    def __call__(self, port, host='localhost', data=None, timeout=5):
        """Uses reliable request-reply to send data to a zmq REP socket, and returns the reply"""
        # We cache the socket so as to not exhaust ourselves of tcp
        # ports. However if a different server is in use, we need a new
        # socket. Also if we don't have a socket, we also need a new one:
        if not hasattr(self.local,'sock') or gethostbyname(host) != self.local.host or int(port) != self.local.port:
            self.new_socket(host,port)
        if self.type == 'multipart' and isinstance(data,str):
            # Wrap up a single string into a list so it doesn't get sent
            # as one character per message!
            data = [data]
        try:
            self.local.send(data, zmq.NOBLOCK)
            events = self.local.poller.poll(timeout*1000) # convert timeout to ms
            if events:
                response = self.local.recv()
            else:
                # The server hasn't replied. We don't know what it's doing,
                # so we'd better stop using this socket in case late messages
                # arrive on it in the future:
                raise zmq.ZMQError('No response from server: timed out')
            if isinstance(response, Exception):
                raise response
            else:
                return response
        except:
            # Any exceptions, we want to stop using this socket:
            del self.local.sock
            raise
            
# Instantiate our zmq_get functions:        
zmq_get = ZMQGet('pyobj')
zmq_get_multipart = ZMQGet('multipart')
zmq_get_raw = ZMQGet('raw')

class ZMQPush(object):
    def __init__(self,type='pyobj'):
        self.local = threading.local()
        self.type = type
        
    def new_socket(self,host,port):
        # Every time there is an exception, we need to create and
        # bind a new socket. Also, we have a separate socket for each
        # thread. Also a new socket if there is a different host or port:
        self.local.host = gethostbyname(host)
        self.local.port = int(port)
        context = zmq.Context.instance()
        self.local.sock = context.socket(zmq.PUSH)
        self.local.sock.setsockopt(zmq.LINGER, 0)
        self.local.sock.connect('tcp://%s:%d'%(self.local.host, self.local.port))
        # Different send/recv methods depending on the desired protocol:
        if self.type == 'pyobj':
            self.local.send = self.local.sock.send_pyobj
            self.local.recv = self.local.sock.recv_pyobj
        elif self.type == 'multipart':
            self.local.send = self.local.sock.send_multipart
            self.local.recv = self.local.sock.recv_multipart
        elif self.type == 'raw':
            self.local.send = self.local.sock.send
            self.local.recv = self.local.sock.recv
            
    def __call__(self, port, host='localhost', data=None, timeout=5):
        # We cache the socket so as to not exhaust ourselves of tcp
        # ports. However if a different server is in use, we need a new
        # socket. Also if we don't have a socket, we also need a new one:
        if not hasattr(self.local,'sock') or gethostbyname(host) != self.local.host or int(port) != self.local.port:
            self.new_socket(host,port)
        if self.type == 'multipart' and isinstance(data,str):
            # Wrap up a single string into a list so it doesn't get sent
            # as one character per message!
            data = [data]
        try:
            self.local.send(data, zmq.NOBLOCK)
        except:
            # Any exceptions, we want to stop using this socket:
            del self.local.sock
            raise

# Instantiate our zmq_push functions:        
zmq_push = ZMQPush('pyobj')
zmq_push_multipart = ZMQPush('multipart')
zmq_push_raw = ZMQPush('raw')
            
class HeartbeatServer(object):
    """A server which receives messages from clients and echoes them
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
        try:
            zmq.device(zmq.FORWARDER, self.sock, self.sock)
        except Exception:
            # Shutting down:
            return

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

    def __init__(self, port, streamname='stdout'):
        self.streamname = streamname
        self.real_stream = getattr(sys,streamname)
        self.fileno = self.real_stream.fileno
        self.local = threading.local()
        self.port = port
    
    def new_socket(self):
        # One socket per thread, so we don't have to acquire a lock
        # to send:
        context = zmq.Context.instance()
        self.local.sock = context.socket(zmq.PUSH)
        self.local.sock.setsockopt(zmq.LINGER, 0)
        self.local.sock.connect('tcp://127.0.0.1:%d'%self.port)
            
    def connect(self):
        setattr(sys,self.streamname,self)
    
    def disconnect(self):
        setattr(sys,self.streamname,self.real_stream)
            
    def write(self, s):
        if not hasattr(self.local, 'sock'):
            self.new_socket()
        self.local.sock.send_multipart([self.streamname, s.encode()])
        
    def close(self):
        self.disconnect()
        self.real_stream.close()
        
    def flush(self):
        pass
        
            
def subprocess_with_queues(path, output_redirection_port=0):

    to_child = context.socket(zmq.PUSH)
    from_child = context.socket(zmq.PULL)
    to_self = context.socket(zmq.PUSH)
    
    port_from_child = from_child.bind_to_random_port('tcp://127.0.0.1')
    to_self.connect('tcp://127.0.0.1:%s'%port_from_child)
    
    heartbeat_port = heartbeat_server.run()
    
    child = subprocess.Popen([sys.executable, '-u', path, str(port_from_child), 
                             str(heartbeat_port), str(output_redirection_port)])
    
    port_to_child = from_child.recv()
    to_child.connect('tcp://127.0.0.1:%s'%port_to_child)
    
    to_child = WriteQueue(to_child)
    from_child = ReadQueue(from_child, to_self)
    
    return to_child, from_child, child
    
def setup_connection_with_parent(lock=False):
    port_to_parent = int(sys.argv[1])
    port_to_heartbeat_server = int(sys.argv[2])
    output_redirection_port = int(sys.argv[3])
    
    to_parent = context.socket(zmq.PUSH)
    from_parent = context.socket(zmq.PULL)
    to_self = context.socket(zmq.PUSH)
        
    port_from_parent = from_parent.bind_to_random_port('tcp://127.0.0.1')
    to_self.connect('tcp://127.0.0.1:%s'%port_from_parent)
    
    to_parent.connect("tcp://127.0.0.1:%s"%port_to_parent)
    to_parent.send(str(port_from_parent))
    
    from_parent = ReadQueue(from_parent, to_self)
    to_parent = WriteQueue(to_parent)
    
    if output_redirection_port: # zero indicates no output redirection
        stdout = OutputInterceptor(output_redirection_port)
        stderr = OutputInterceptor(output_redirection_port,'stderr')
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
