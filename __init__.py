import os
import socket
import threading
import time

import zmq

DEFAULT_TIMEOUT = 15 # seconds
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
        if timeout is None:
            timeout = self.RESPONSE_TIMEOUT
        else:
            timeout = 1000*timeout # convert to ms
        if not hasattr(self.local,'sock'):
            self.new_socket()
        self.local.sock.send('hello',zmq.NOBLOCK)
        events = self.local.poller.poll(timeout)
        if events:
            response = self.local.sock.recv()
            if response == 'hello':
                return
        del self.local.sock
        raise zmq.ZMQError('No response from server: timed out')
        
    def acquire(self, key, timeout):
        if not hasattr(self.local,'sock'):
            self.new_socket()
        while True:
            messages = ('acquire',str(key),self.local.client_id, str(timeout))
            self.local.sock.send_multipart(messages, zmq.NOBLOCK)
            events = self.local.poller.poll(self.RESPONSE_TIMEOUT)
            if not events:
                del self.local.sock
                raise zmq.ZMQError('No response from server: timed out')
            else:    
                response = self.local.sock.recv()
                if response == 'ok':
                    break
                elif response == 'retry':
                    continue
                else:
                    raise zmq.ZMQError(response)

        
    def release(self, key):
        if not hasattr(self.local,'sock'):
            self.new_socket()
        messages = ('release',str(key),self.local.client_id)
        self.local.sock.send_multipart(messages)
        events = self.local.poller.poll(self.RESPONSE_TIMEOUT)
        if not events:
            del self.local.sock
            raise zmq.ZMQError('No response from server: timed out')
        else:    
            response = self.local.sock.recv()
            if response == 'ok':
                return
            else:
                raise zmq.ZMQError(response)


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


class Lock(object):
    def __init__(self, key):
        self.key = key
        self.held = False
        
    def acquire(self):
        self.held = True
        acquire(self.key)
        
    def release(self):
        release(self.key)
        self.held = False
                
    def __enter__(self):
        self.acquire()
        
    def __exit__(self, *args):
        self.release()
        
    def __del__(self):
        if self.held:
            self.release()

class ProcessLock(object):
    def __init__(self, key):
        self.key = key
        self.held = False
        
    def acquire(self):
        acquire(self.key)
        self.held = True
        
    def release(self):
        release(self.key)
        self.held = False
                
    def __enter__(self):
        self.acquire()
        
    def __exit__(self, *args):
        self.release()
        
    def __del__(self):
        if self.held:
            self.release()

        
def ping(timeout=1):
    start_time = time.time()
    try:
        _zmq_lock_client.say_hello(timeout)
        return (time.time() - start_time)*1000
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
    return ping(timeout)

try:
    import ConfigParser
    from LabConfig import LabConfig
    host = LabConfig().get('servers','zlock')
    port = LabConfig().get('ports','zlock')
except (ImportError, IOError, ConfigParser.NoOptionError):
    # Couldn't get connection settings.  Try localhost, default
    # port. Short timeout, don't want to waste time if it's not there:
    try:
        connect('localhost', timeout=0.05)
        host, port = 'locahost', DEFAULT_PORT
    except zmq.ZMQError:
        # The user will have to call connect() themselves:
        host = port = None
    
if host is not None:
    # Automatically connect to the settings from LabConfig:
    connect(host,port)

    
    
    
