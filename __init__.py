import sys
import os
import socket
import threading
import time
import weakref
import atexit

import zmq

DEFAULT_TIMEOUT = 30 # seconds
DEFAULT_PORT = 7339

process_identifier_prefix = ''
thread_identifier_prefix = threading.local()

def name_change_checks():
    if Lock.instances:
        raise RuntimeError('Cannot change process/thread name while Locks are in use.' +
                           'Please change names before instantiating Lock objects.')
    if '_zmq_lock_client' in globals():
        # Clear thread local data so that the client id is re-generated in all threads:
        _zmq_lock_client.local = threading.local()
                     
def set_client_process_name(name):
    global process_identifier_prefix
    name_change_checks()
    process_identifier_prefix = name + '-'
        
def set_client_thread_name(name):
    name_change_checks()
    thread_identifier_prefix.prefix = name + '-'
    
def get_client_id():
    try:
        prefix = thread_identifier_prefix.prefix
    except AttributeError:
        prefix = thread_identifier_prefix.prefix = ''
    thread_identifier = prefix + threading.current_thread().name
    process_identifier = process_identifier_prefix + str(os.getpid())
    host_name = socket.gethostname()
    return ':'.join([host_name, process_identifier,thread_identifier])

def raise_exception_in_thread(exc_info):
    """Raises an exception in a thread"""
    def f(exc_info):
        raise exc_info[0], exc_info[1], exc_info[2]
    threading.Thread(target=f,args=(exc_info,)).start()
            
class ZMQLockClient(object):

    RESPONSE_TIMEOUT = 2000
    
    def __init__(self, host, port):
        self.host = socket.gethostbyname(host)
        self.port = port
        self.lock = threading.Lock()
        # We'll store one zmq socket/poller for each thread, with thread
        # local storage:
        self.local = threading.local()
        self.shutdown_complete = threading.Event()
         
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
        self.local.client_id = get_client_id()
        
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
            raise zmq.ZMQError('No response from zlock server: timed out')
        except:
            self.local.sock.close(linger=False)
            del self.local.sock
            raise
    
    def status(self):
        try:
            if not hasattr(self.local,'sock'):
                self.new_socket()
            self.local.sock.send('status',zmq.NOBLOCK)
            events = self.local.poller.poll(self.RESPONSE_TIMEOUT)
            if events:
                response = self.local.sock.recv()
                if response.startswith('ok'):
                    return response
                raise zmq.ZMQError(response)
            raise zmq.ZMQError('No response from zlock server: timed out')
        except:
            self.local.sock.close(linger=False)
            del self.local.sock
            raise
            
    def clear(self, clear_all):
        try:
            if not hasattr(self.local,'sock'):
                self.new_socket()
            self.local.sock.send_multipart(['clear', str(clear_all)],zmq.NOBLOCK)
            events = self.local.poller.poll(self.RESPONSE_TIMEOUT)
            if events:
                response = self.local.sock.recv()
                if response == 'ok':
                    return
                raise zmq.ZMQError(response)
            raise zmq.ZMQError('No response from zlock server: timed out')
        except:
            self.local.sock.close(linger=False)
            del self.local.sock
            raise
            
    def acquire(self, key, timeout, retries=1):
        if not hasattr(self.local,'sock'):
            self.new_socket()
        try:
            while True:
                messages = ('acquire', str(key), self.local.client_id, str(timeout))
                self.local.sock.send_multipart(messages, zmq.NOBLOCK)
                events = self.local.poller.poll(self.RESPONSE_TIMEOUT)
                if not events:
                    if retries > 0:
                        self.local.sock.close(linger=False)
                        del self.local.sock
                        return self.acquire(key, timeout, retries-1)
                    raise zmq.ZMQError('No response from zlock server: timed out')
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
        
    def release(self, key, client_id, retries=1):
        if not hasattr(self.local,'sock'):
            self.new_socket()
        try:
            if client_id is None:
                client_id = self.local.client_id
            messages = ('release', str(key), client_id)
            self.local.sock.send_multipart(messages)
            events = self.local.poller.poll(self.RESPONSE_TIMEOUT)
            if not events:
                if retries > 0:
                    self.local.sock.close(linger=False)
                    del self.local.sock
                    return self.release(key, client_id, retries-1)
                raise zmq.ZMQError('No response from zlock server: timed out')
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
    
class Lock(object):

    instances = weakref.WeakValueDictionary()
    class_lock = threading.Lock()
    
    def __new__(cls, key, *args, **kwargs):
        with cls.class_lock:
            try:
                instance = cls.instances[key]
            except KeyError:
                instance = object.__new__(cls)
                cls.instances[key] = instance
            return instance
            
    def __init__(self, key):
        with self.class_lock:
            if not hasattr(self,'key'):
                self.key = key
                self.local_lock = threading.RLock()
                self.recursion_level=0
                try:
                    _zmq_lock_client
                except NameError:
                    raise RuntimeError('Not connected to a zlock server')
        
    def acquire(self, retries=1):
        self.local_lock.acquire()
        self.recursion_level += 1
        if self.recursion_level==1:
            try:
                acquire(self.key, retries=retries)
            except:
                self.recursion_level =- 1
                self.local_lock.release()
                raise
            
    def release(self, retries=1):
        if self.recursion_level==0:
            raise RuntimeError('cannot release un-acquired lock')
        try:
            if self.recursion_level==1:
                release(self.key, retries=retries)
        finally:
            # Always release the local lock, even if we failed to release
            # the network lock:
            self.recursion_level -= 1
            self.local_lock.release()
        
    def __enter__(self):
        self.acquire()
        
    def __exit__(self, type, value, traceback):
        self.release()
        
        
class NetworkOnlyLock(object):

    instances = weakref.WeakValueDictionary()
    class_lock = threading.Lock()
    
    def __new__(cls, key, *args, **kwargs):
        with cls.class_lock:
            try:
                instance = cls.instances[key]
            except KeyError:
                instance = object.__new__(cls)
                cls.instances[key] = instance
            return instance
            
    def __init__(self, key):
        with self.class_lock:
            if not hasattr(self,'key'):
                self.key = key
                # Get the Lock for this key:
                self.lock = Lock(key)
        
    def acquire(self):
        with self.lock.local_lock:
            acquire(self.key)
            self.lock.recursion_level += 1
            
    def release(self):
        with self.lock.local_lock:
            if self.lock.recursion_level != 1:
                raise RuntimeError('cannot release network lock whilst local locks still held')
            try:
                release(self.key)
            finally:
                self.lock.recursion_level = 0
        
    def __enter__(self):
        self.acquire()
        
    def __exit__(self, type, value, traceback):
        self.release()


def acquire(key, timeout=None, retries=1):
    """Acquire a lock identified by key, for a specified time in
    seconds. Blocks until success, raises exception if the server isn't
    responding"""
    if timeout is None:
        timeout = DEFAULT_TIMEOUT
    try:
        _zmq_lock_client.acquire(key, timeout, retries=retries)
    except NameError:
        raise RuntimeError('Not connected to a zlock server')
        
def release(key, client_id=None, retries=1):
    """Release the lock identified by key. Raises an exception if the
    lock was not held, or was held by someone else, or if the server
    isn't responding. If client_id is provided, one thread can release
    the lock on behalf of another, but this should not be the normal
    usage."""
    try:
        _zmq_lock_client.release(key, client_id, retries=retries)
    except NameError:
        raise RuntimeError('Not connected to a zlock server')

def ping(timeout=1):
    try:
        return _zmq_lock_client.say_hello(timeout)
    except NameError:
        raise RuntimeError('Not connected to a zlock server')

def status():
    try:
        return _zmq_lock_client.status()
    except NameError:
        raise RuntimeError('Not connected to a zlock server')
        
def clear(clear_all):
    try:
        return _zmq_lock_client.clear(clear_all)
    except NameError:
        raise RuntimeError('Not connected to a zlock server')
              
def set_default_timeout(t):
    """Sets how long the locks should be acquired for before the server
    times them out and allows other clients to acquire them. Attempting
    to release them will then result in an exception."""
    global DEFAULT_TIMEOUT
    DEFAULT_TIMEOUT = t

def guess_server_address():
    host, port = None, None

    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        port = sys.argv[2]

    if host is None or port is None:    
        try:
            import ConfigParser
            from LabConfig import LabConfig
            config = LabConfig()
        except (ImportError, IOError):
            host, port = 'localhost', zlock.DEFAULT_PORT
        else:
            if host is None:
                try:
                    host = config.get('servers','zlock')
                except ConfigParser.NoOptionError:
                    host = 'localhost'
            if port is None:
                try:
                    port = config.get('ports','zlock')
                except ConfigParser.NoOptionError:
                    port = zlock.DEFAULT_PORT
    return host, port
        
def connect(host='localhost', port=DEFAULT_PORT, timeout=1):
    """This method should be called at program startup, it establishes
    communication with the server and ensures it is responding"""
    global _zmq_lock_client
    _zmq_lock_client = ZMQLockClient(host, port)
    # We ping twice since the first does initialisation and so takes
    # longer. The second will be more accurate:
    ping(timeout)
    return ping(timeout)

