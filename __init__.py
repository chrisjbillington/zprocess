import os
import socket
import threading
import time
import weakref

import zmq

DEFAULT_TIMEOUT = 60 # seconds
DEFAULT_PORT = 7339

class ZMQLockClient(object):

    RESPONSE_TIMEOUT = 2000
    
    def __init__(self, host, port):
        self.host = socket.gethostbyname(host)
        self.port = port
        self.lock = threading.Lock()
        # We'll store one zmq socket/poller for each thread, with thread local storage:
        self.local = threading.local()
        
    def new_socket(self):
        # Every time the REQ/REP cadence is broken, we need to create
        # and bind a new socket to get it back on track. Also, we have
        # a separate socket for each thread:
        context = zmq.Context.instance()
        self.local.sock = context.socket(zmq.REQ)
        self.local.sock.setsockopt(zmq.LINGER, 0)
        self.local.poller = zmq.Poller()
        self.local.poller.register(self.local.sock, zmq.POLLIN)
        self.local.sock.connect('tcp://%s:%s'%(self.host, str(self.port)))    
        self.local.client_id = self.client_id()
    
    def client_id(self):
        host_name = socket.gethostname()
        process_id = str(os.getpid())
        thread_name= threading.current_thread().name
        return ':'.join([host_name,process_id,thread_name])
    
    def say_hello(self,timeout=None):
        """Ping the server to test for a response"""
        try:
            if timeout is None:
                timeout = self.RESPONSE_TIMEOUT
            else:
                timeout = 1000*timeout # convert to ms
            if not hasattr(self.local,'sock'):
                self.new_socket()
            start_time = time.time()
            self.local.sock.send('hello',zmq.NOBLOCK)
            events = self.local.poller.poll(timeout)
            if events:
                response = self.local.sock.recv()
                if response == 'hello':
                    return round((time.time() - start_time)*1000,2)
            raise zmq.ZMQError('No response from server: timed out')
        except:
            self.local.sock.close(linger=False)
            del self.local.sock
            raise
            
    def acquire(self, key, timeout):
        if not hasattr(self.local,'sock'):
            self.new_socket()
        try:
            while True:
                messages = ('acquire',str(key),self.local.client_id, str(timeout))
                self.local.sock.send_multipart(messages, zmq.NOBLOCK)
                events = self.local.poller.poll(self.RESPONSE_TIMEOUT)
                if not events:
                    raise zmq.ZMQError('No response from server: timed out')
                else:    
                    response = self.local.sock.recv()
                    if response == 'ok':
                        break
                    elif response == 'retry':
                        continue
                    else:
                        raise zmq.ZMQError(response)
        except: 
            self.local.sock.close(linger=False)
            del self.local.sock
            raise

        
    def release(self, key):
        if not hasattr(self.local,'sock'):
            self.new_socket()
        try:
            messages = ('release',str(key),self.local.client_id)
            self.local.sock.send_multipart(messages)
            events = self.local.poller.poll(self.RESPONSE_TIMEOUT)
            if not events:
                raise zmq.ZMQError('No response from server: timed out')
            else:    
                response = self.local.sock.recv()
                if response == 'ok':
                    return
                else:
                    raise zmq.ZMQError(response)
        except:
            self.local.sock.close(linger=False)
            del self.local.sock
            raise

def acquire(key, timeout=None):
    """Acquire a lock identified by key, for a specified time in
    seconds. Blocks until success, raises exception if the server isn't
    responding"""
    if timeout is None:
        timeout = DEFAULT_TIMEOUT
    try:
        _zmq_lock_client.acquire(key, timeout)
    except NameError:
        raise RuntimeError('Not connected to a zlock server')
        
def release(key):
    """Release the lock identified by key. Raises an exception if the
    lock was not held, or was held by someone else, or if the server
    isn't responding"""
    try:
        _zmq_lock_client.release(key)
    except NameError:
        raise RuntimeError('Not connected to a zlock server')

def ping(timeout=1):
    start_time = time.time()
    try:
        return _zmq_lock_client.say_hello(timeout)
    except NameError:
        raise RuntimeError('Not connected to a zlock server')
        
def set_default_timeout(t):
    global DEFAULT_TIMEOUT
    DEFAULT_TIMEOUT = t
          
def connect(host='localhost', port=DEFAULT_PORT, timeout=1):
    """This method should be called at program startup, it establishes
    communication with the server and ensures it is responding"""
    global _zmq_lock_client                 
    _zmq_lock_client = ZMQLockClient(host, port)
    # We ping twice since the first does initialisation and so takes
    # longer. The second will be more accurate:
    ping(timeout)
    return ping(timeout)
    
    
class KeyedSingletons(object):
    """A superclass for classes with the requirement that there not be
    more than one instance with the same key. If an instance with the
    same key is instantiated, the existing instance is returned instead.
    __init__ is called regardless, so it should have an if statement
    checking for non-existance of some attribute indicating previous
    initialisation before proceeding

    Furthermore subclasses should acquire the class_lock during their
    __init__ methods so that there can be no possibility of two __init__
    methods running simultaneously. """

    instances = weakref.WeakValueDictionary()
    class_lock = threading.Lock()
    
    def __new__(cls, key, *args, **kwargs):
        with cls.class_lock:
            return  cls.instances.setdefault(key,object.__new__(cls))
        
        
class Lock(KeyedSingletons):
            
    def __init__(self, key):
        with self.class_lock:
            if not hasattr(self,'key'):
                self.key = key
                self.lock = threading.Lock()
                self.local_only = False
        
    def acquire(self):
        self.lock.acquire()
        if not self.local_only:
            acquire(self.key)
        
    def release(self):
        if not self.local_only:
            release(self.key)
        self.lock.release()
        
    def __enter__(self):
        self.acquire()
        
    def __exit__(self, type, value, traceback):
        self.release()
        
        
class NetworkOnlyLock(KeyedSingletons):
        
    def __init__(self, key):
        with self.class_lock:
            if not hasattr(self,'key'):
                self.key = key
                # Get the Lock for this key:
                self.lock = Lock(key)
        
    def acquire(self):
        with self.lock.lock:
            acquire(self.key)
            self.lock.local_only = True
            
    def release(self):
        with self.lock.lock:
            release(self.key)
            self.lock.local_only = False
        
    def __enter__(self):
        self.acquire()
        
    def __exit__(self, type, value, traceback):
        self.release()
        
