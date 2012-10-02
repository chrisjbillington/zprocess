import os
import sys
import traceback
import time
import logging, logging.handlers

import zmq


DEFAULT_PORT = 7339   
RETRY_INTERVAL = .1 # sec
MAX_RESPONSE_TIME = 1 # sec
LOGGING = True

def setup_logging():
    logger = logging.getLogger('ZLock')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(message)s')
    if sys.stdout.isatty():
        terminalhandler = logging.StreamHandler(sys.stdout)
        terminalhandler.setFormatter(formatter)
        terminalhandler.setLevel(logging.DEBUG)
        logger.addHandler(terminalhandler)
    else:
        # Prevent bug on windows where writing to stdout without a command
        # window causes a crash:
        sys.stdout = sys.stderr = open(os.devnull,'w')
    if os.name == 'nt':
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'zlock.log')
    else:
        path = '/var/log/zlock.log'
    if os.access(path,os.W_OK):
        handler = logging.handlers.RotatingFileHandler(path, maxBytes=1024*1024*50)
        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)
    else:
        logger.warning('Do not have permission to write to log file %s. '%path + 
                       'Only terminal logging will be output.')
    return logger


class ZMQLockServer(object):
    
    def __init__(self, port):
        self.port = int(port)
        
        # A dictionary of locks currently held by clients:
        self.held_locks = {}
        
        # We have an extra ROUTER-DEALER layer before our REP socket
        # so that we can delay replying to some requests until after
        # other requests have been processed. This is useful in the case
        # of lock contention: instead of replying saying 'sorry, the
        # lock is already held', we can instead wait until some other
        # requests have been processed (which may release the lock)
        # before replying.

        context = zmq.Context.instance()
        self.router = context.socket(zmq.ROUTER)
        self.dealer = context.socket(zmq.DEALER)
        self.sock = context.socket(zmq.REP)
        
        self.poller = zmq.Poller()
        self.poller.register(self.router, zmq.POLLIN)
        
        # Bind the router to the outside world and connect the dealer
        # to the REP socket via inproc:
        self.router.bind('tcp://0.0.0.0:%d'%self.port)
        self.sock.bind('inproc://to-rep-socket')
        self.dealer.connect('inproc://to-rep-socket')
        
        self.handlers = {'hello': self.hello, 'acquire': self.acquire, 'release': self.release}
        
        
    def hello(self):
        if LOGGING: logger.info('someone said hello')
        return 'hello'
            
    def acquire(self, key, client_id, timeout):
        if (key not in self.held_locks) or self.held_locks[key]['expiry'] < time.time():
            self.held_locks[key] = {'client_id': client_id, 'expiry': float(timeout) + time.time()}
            if LOGGING: logger.info('%s acquired %s'%(client_id, key))
            return 'ok'
        if LOGGING: logger.info('%s is waiting to acquire %s'%(client_id, key))
        return 'retry'
            
    def release(self, key, client_id):
        if key in self.held_locks:
            if self.held_locks[key]['client_id'] == client_id and self.held_locks[key]['expiry'] > time.time():
                del self.held_locks[key]
                if LOGGING: logger.info('%s released %s'%(client_id, key))
                return 'ok'
        raise RuntimeError('lock %s timed out or was not acquired prior to release by %s'%(key, client_id))
    
    def handle_one_request(self):
        messages = self.sock.recv_multipart()
        # Handle the request:
        request, args = messages[0], messages[1:]
        try:
            response = self.handlers[request](*args)
        except Exception:
            response = traceback.format_exc()
            logger.error('%s:\n%s'%(str(messages), response))
        self.sock.send(response)
        return response
                          
    def run(self):
        if LOGGING: logger.info('This is zlock server, running on port %d'%self.port)
        unprocessed_messages = []
        while True:
            # Wait at most RETRY_INTERVAL for incoming request messages:
            events = self.poller.poll(RETRY_INTERVAL*1000)
            if events:
                # If there was a new request, this will be processed
                # first, being prepended to unprocessed_messages.  Then we
                # will process any other requests that are waiting,
                # in case the locks they are waiting on have timed out,
                # or MAX_RESPONSE_TIME has elapse and we need to reply
                # to their client.
                new_request_message = self.router.recv_multipart()
                unprocessed_messages.insert(0, (new_request_message, time.time() + MAX_RESPONSE_TIME))
            # Process all waiting request messages:
            for request_message, expiry in unprocessed_messages[:]:
                self.dealer.send_multipart(request_message)
                response = self.handle_one_request()
                reply_message = self.dealer.recv_multipart()
                if response == 'retry' and time.time() < expiry:
                    # Lock contention. Lock acquisition will be
                    # retried after other requests are processed, or
                    # every RETRY_INTERVAL. Don't give the client a
                    # response yet.
                    print time.time(), expiry
                    continue
                else:
                    # If success or error, tell the client about it. Or
                    # if we have already been retrying their request for
                    # MAX_RESPONSE_TIME, forward the 'retry' response
                    # to them.
                    unprocessed_messages.remove((request_message, expiry))
                    if LOGGING: logger.info('Replying...')
                    self.router.send_multipart(reply_message)
                    
                    
if __name__ == '__main__':
    if LOGGING: logger = setup_logging()
    
    try:
        import ConfigParser
        from LabConfig import LabConfig
        port = LabConfig().get('ports','zlock')
    except (ImportError, IOError, ConfigParser.NoOptionError):
        if LOGGING: logger.warning("Couldn't get port setting from LabConfig. Using default port")
        port = DEFAULT_PORT
    
    server = ZMQLockServer(port)
    while True:
        try:
            server.run()
        except KeyboardInterrupt:
            if LOGGING: logger.info('KeyboardInterrupt, stopping')
            break
        except Exception:
            message = traceback.format_exc()
            if LOGGING: logger.critical('unhandled exception, attempting to restart:\n%s'%message)
            # Close all sockets:
            context = zmq.Context.instance()
            context.destroy(linger=False)
            # Re-initialise the server:
            server = ZMQLockServer(port)
            
            
            
    
