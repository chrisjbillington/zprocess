import sys
import os
import socket
import threading
import time
import weakref
import atexit

import zmq

DEFAULT_TIMEOUT = 30 # seconds
MIN_CACHE_TIME = 0.1 # seconds
MAX_CACHE_TIME = 1 # seconds
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
        
    def new_delayed_release_socket(self):
        # Each new thread that uses the delayed release mechanism needs
        # its own socket for it. I usually don't take very seriously
        # the zeromq cohort's fire and brimstone tirade about not using
        # sockets from multiple threads, I'm sure you can (with a lock of
        # course, though I hear even that isn't required in later zeromq
        # versions) but they just don't recommend it since it disagrees
        # with their programming philosophy. I happen to agree with their
        # non-state-sharing philosophy but still prefer not to be lied
        # to. So I'll usually use a socket from multiple threads, if it
        # suits. However in this case I don't want a single socket with a
        # lock to serialize access to it, because I actually want separate
        # threads to be able to use this mechanism simultaneously. So
        # we'll have one socket per thread, instantiated via a call to
        # this method the first time its needed by the calling thread.
        context = zmq.Context.instance()
        self.local.delayed_release_req = context.socket(zmq.REQ)
        self.local.delayed_release_req.setsockopt(zmq.LINGER, 0)
        self.local.delayed_release_req.connect('inproc://delayed-release')
        
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
            
    def acquire(self, key, timeout):
        if not hasattr(self.local,'sock'):
            self.new_socket()
        try:
            while True:
                messages = ('acquire', str(key), self.local.client_id, str(timeout))
                self.local.sock.send_multipart(messages, zmq.NOBLOCK)
                events = self.local.poller.poll(self.RESPONSE_TIMEOUT)
                if not events:
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

        
    def release(self, key, client_id):
        if not hasattr(self.local,'sock'):
            self.new_socket()
        try:
            if client_id is None:
                client_id = self.local.client_id
            messages = ('release', str(key), client_id)
            self.local.sock.send_multipart(messages)
            events = self.local.poller.poll(self.RESPONSE_TIMEOUT)
            if not events:
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
    
    def delayed_release(self, key, client_id):
        if not hasattr(self.local,'delayed_release_req'):
            self.new_delayed_release_socket()
        self.local.delayed_release_req.send_multipart(['add', key, client_id])
        self.local.delayed_release_req.recv()
        
    def cancel_delayed_release(self, key):
        if not hasattr(self.local,'delayed_release_req'):
            self.new_delayed_release_socket()
        self.local.delayed_release_req.send_multipart(['cancel', key, ''])
        self.local.delayed_release_req.recv()

    def shutdown_delayed_release(self):
        if not hasattr(self.local,'delayed_release_req'):
            self.new_delayed_release_socket()
        self.local.delayed_release_req.send_multipart(['shutdown', '', ''])
        self.shutdown_complete.wait()
            
            
def _delayed_release_loop(rep_sock, poller):
    # Runs in a thread releasing locks after their requisite
    # delays.  Raises any exceptions in a separate thread and keeps
    # running. Gets requests for delayed releasing via a REP socket
    # bound on inproc communication. The corresponding REQ socket
    # has requests put into it in the delayed_release function.
    poll_duration = -1
    locks_to_release = {}
    release_times = {}
    max_release_times = {}
    shutting_down = False
    while True:
        events = poller.poll(poll_duration)
        if events:
            request, key, client_id = rep_sock.recv_multipart()
            if request == 'cancel' and key in locks_to_release:
                release_times[key] = max_release_times[key]
            elif request == 'add':
                if key not in locks_to_release:
                    # It's important to hold onto a reference to
                    # the Lock so that it's not garbage collected
                    # and its state is maintained even if there are
                    # no other instances left:
                    lock = Lock(key)
                    locks_to_release[key] = (lock, client_id)
                    max_release_times[key] = lock.acquire_time + MAX_CACHE_TIME
                release_time = time.time() + MIN_CACHE_TIME
                release_times[key] = min(release_time, max_release_times[key])
            elif request == 'shutdown':
                shutting_down = True
            # Only reply once we have a reference to the lock, otherwise
            # it might be garbage collected before we do:
            rep_sock.send('')
            
        for key, release_time in release_times.items():
            if (time.time() > release_time) or shutting_down:
                lock, client_id = locks_to_release[key]
                with lock.lock:
                    lock.local_only = False
                    try:
                        _zmq_lock_client.release(key, client_id)
                    except Exception:
                        # Raise the exception in a separate thread
                        # so the user knows about it but we can
                        # keep running:
                        raise_exception_in_thread(sys.exc_info())
                    finally:
                        del release_times[key]
                        del locks_to_release[key]
                        del max_release_times[key]
        # How long until the next lock needs releasing?
        if release_times:
            now = time.time()
            poll_duration = int(min(t - now for t in release_times.values())*1000)
            # Make sure it's non-negative if we've actually gone past
            # the time we're supposed to release it:
            poll_duration = max(poll_duration,0)
        else:
            # poll will block until there are events:
            poll_duration = -1
        if shutting_down:
            _zmq_lock_client.shutdown_complete.set()
            return
                        
@atexit.register
def release_delayed_locks():
    # if the interpreter is shutting down, we don't want to delay
    # releasing those locks any more, or they won't be released at all!
    try:
        _zmq_lock_client
        _delayed_release_thread
    except NameError:
        # We're not connected, it doesn't matter:
        return
    _zmq_lock_client.shutdown_delayed_release()
        
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
                self.lock = threading.Lock()
                self.local_only = False
                try:
                    _zmq_lock_client
                except NameError:
                    raise RuntimeError('Not connected to a zlock server')
        
    def acquire(self):
        if self.local_only and MIN_CACHE_TIME:
            # We can't hold self.lock here as the lock releaser may try
            # to acquire it (in order to release the network lock) before
            # seeing to our request.  This would deadlock. No matter,
            # if we're too late with our cancel request and the delayed
            # release has already been performed then the below block
            # will notice that self.local_only == False after acquiring
            # self.lock and all will be well.  And the delayed lock
            # releaser ignores cancel requests for locks it has already
            # released, so there will be no problem there either.
            _zmq_lock_client.cancel_delayed_release(self.key)
        self.lock.acquire()
        if not self.local_only:
            try:
                acquire(self.key)
                self.acquire_time = time.time()
            except:
                # If we fail to acquire the network lock, don't acquire
                # the local lock either:
                self.lock.release()
                raise
            
    def release(self):
        try:
            if not self.local_only:
                if MIN_CACHE_TIME:
                    _zmq_lock_client.delayed_release(self.key, get_client_id())
                    self.local_only = True
                else:
                    release(self.key)
        finally:
            # Always release the local lock, even if we failed to release
            # the network lock:
            self.lock.release()
        
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
        
def release(key, client_id=None):
    """Release the lock identified by key. Raises an exception if the
    lock was not held, or was held by someone else, or if the server
    isn't responding. If client_id is provided, one thread can release
    the lock on behalf of another, but this should not be the normal
    usage. It is included mainly for for use by delayed_releaser."""
    try:
        _zmq_lock_client.release(key, client_id)
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

def set_cache_time(min=0, max=30):
    """Sets how long in seconds after a lock is locally released it should
    be released on the network. If this is nonzero, then any local threads
    acquiring the lock before it is released on the network but after
    it is released locally, will instead only acquire a local lock (and
    the previously scheduled network release will be cancelled). This
    is so that rapid acquisition and release need not go out to the
    network every time. Once the total hold time of the network lock
    passes MAX_CACHE_TIME, it will be released regardless."""
    global MIN_CACHE_TIME
    global MAX_CACHE_TIME
    MIN_CACHE_TIME = min
    MAX_CACHE_TIME = max

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
    try:
        global _delayed_release_thread
        _delayed_release_thread
    except NameError:
        # Start a thread for releasing the locks on the network after
        # a delay. It uses inproc req/rep to communicate with other
        # threads. Unlike other protocols, inproc sockets must bind before
        # clients connect. So we'd better create this socket, bind it and
        # pass it to the thread. We can't instantiate it in the thread
        # itself and wait for it to bind, as socket instantiation
        # in pyzmq implicity involves an import, which leads to a
        # deadlock if another import is in progress in another thread (see
        # http://docs.python.org/library/threading.html#importing-in-threaded-code).
        # Since I want other modules to be able to call connect() whilst
        # they are being imported, this seemed like a logical solution.
        context = zmq.Context.instance()
        rep_sock = context.socket(zmq.REP)
        poller = zmq.Poller()
        poller.register(rep_sock, zmq.POLLIN)
        rep_sock.bind('inproc://delayed-release')
        _delayed_release_thread = threading.Thread(target=_delayed_release_loop,args=(rep_sock, poller))
        _delayed_release_thread.daemon = True
        _delayed_release_thread.start()
    return ping(timeout)

