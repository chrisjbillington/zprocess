import zmq
import traceback
import threading
import time
import sys

SERVER_PORT = 7339
RETRY_INTERVAL = 1000 # ms

class ZMQLockServer(object):
    def __init__(self, port, retry_interval):
        self.port = port
        self.retry_interval = retry_interval
        
        # A dictionary of locks currently held by clients:
        self.held_locks = {}
        
        # A lock for serialising access to the above dictionary, so that
        # the thread decrementing locks' time-to-live doesn't modify it
        # at the same time as the main server:
        self.access_lock = threading.Lock()
        
        # The thread which decrements timeouts on held locks, releasing
        # them when it hits zero:
        self.timeout_monitor = threading.Thread(target=self.monitor_timeouts)
        self.timeout_monitor.daemon = True
        self.timeout_monitor.start()
        
    def acquire(self, filepath, uuid, timeout):
        timeout = int(timeout)
        with self.access_lock:
            if filepath in self.held_locks:
                lock = self.held_locks[filepath]
                if lock['uuid'] == uuid:
                    lock['depth'] += 1
                    lock['timeout'] = max(lock['timeout'], timeout)
                    return True, lock['depth']
                else:
                    return False, lock['uuid']
            else:
                lock = {'uuid':uuid, 'timeout': timeout, 'depth': 1}
                self.held_locks[filepath] = lock
                return True, lock['depth']
        
    def release(self, filepath, uuid):
        with self.access_lock:
            if filepath in self.held_locks.copy():
                lock = self.held_locks[filepath]
                if lock['uuid'] == uuid:
                    lock['depth'] -= 1
                    if lock['depth'] == 0:
                        del self.held_locks[filepath]
                        return True, lock['depth']
                    return True, lock['depth']
                else:
                    return False, None
            else:
                return False, None
        
    def run(self):
        context = zmq.Context.instance()
        # We have an extra ROUTER-DEALER layer before our REP socket
        # so that we can monitor for incoming requests from clients
        # on the ROUTER before having sent a response to the current
        # client. Otherwise the REP socket hides this from us.

        router = context.socket(zmq.ROUTER)
        dealer = context.socket(zmq.DEALER)
        sock = context.socket(zmq.REP)
        
        poller = zmq.Poller()
        poller.register(router, zmq.POLLIN)
        
        # Bind the router to the outside world:            
        router.bind('tcp://0.0.0.0:%d'%self.port)

        # Bind the REP socket to an inproc handle:
        sock.bind('inproc://to-rep-socket')
        
        # Connect the dealer to the rep socket:
        dealer.connect('inproc://to-rep-socket')

        while True:
            # Forward a (multipart) message from the router, through the dealer, to the REP socket:
            message = router.recv_multipart()
            dealer.send_multipart(message)
            # Pull the same message out of the REP socket
            messages = sock.recv_multipart()
            try:
                request = messages[0]
                if request == 'hello':
                    sock.send('hello')
                elif request == 'acquire':
                    args = messages[1:]
                    success, data = self.acquire(*args)
                    if success:
                        sock.send_multipart(['ok','lock acquired, re-entry depth %d'%data])
                    else:
                        # Wait until next event, or RETRY_INTERVAL if no
                        # events. The event might be the other client
                        # releasing the lock! This client should retry
                        # immediately.  This is much better than the client
                        # retrying every .1 seconds or something, not knowing
                        # whether there's been any activity on the server:
                        events = poller.poll(self.retry_interval)
                        sock.send_multipart(['retry', 'lock held by uuid %s'%data])
                elif request == 'release':
                    args = messages[1:]
                    success, data = self.release(*args)
                    if success:
                        sock.send_multipart(['ok','lock released, re-entry depth %d'%data])
                    else:
                        sock.send_multipart(['error','lock holding timed out or was never acquired'])
                else:
                    raise ValueError('invalid method: %s'%request)
            except Exception:
                traceback_lines = traceback.format_exception(sys.exc_type, sys.exc_value, sys.exc_traceback)
                message = ''.join(traceback_lines)
                sock.send_multipart(['error',message])
            # And finally, forward the response from the dealer back
            # through the router to the client:
            message = dealer.recv_multipart()
            router.send_multipart(message)
            
    def monitor_timeouts(self):
        while True:
            time.sleep(1)
            with self.access_lock:
                # copy so as not to modify whilst iterating over:
                for filename, lock in self.held_locks.copy().items():
                    lock['timeout'] -= 1
                    if lock['timeout'] <= 0:
                        # lock holding has timed out. release lock:
                        del self.held_locks[filename]
     
if __name__ == '__main__':
    server = ZMQLockServer(SERVER_PORT, RETRY_INTERVAL)
    server.run()
    
