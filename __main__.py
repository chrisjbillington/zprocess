import os
import sys
import traceback
import time
import logging, logging.handlers

import zmq

DEFAULT_PORT = 7339   
RETRY_INTERVAL = 1000 # ms

try:
    import ConfigParser
    from LabConfig import LabConfig
    port = LabConfig().get('ports','zlock')
except (ImportError, IOError, ConfigParser.NoOptionError):
    logger.warning("Couldn't get port setting from LabConfig. Using default port")
    port = DEFAULT_PORT
        
        
def setup_logging():
    logger = logging.getLogger('ZLock')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(name)s: %(message)s')
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
        # so that we can monitor for incoming requests from clients
        # on the ROUTER before having sent a response to the current
        # client. Otherwise the REP socket hides this from us.
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
    
    def hello(self):
        return 'hello'
            
    def acquire(self, filepath, client_id, timeout):
        if (filepath not in self.held_locks) or self.held_locks[filepath]['expiry'] < time.time():
            self.held_locks[filepath] = {'client_id': client_id, 'expiry': float(timeout) + time.time()}
            logger.info('%s acquired %s'%(client_id, filepath))
            return 'ok'
        logger.info('%s is waiting to acquire %s'%(client_id, filepath))
        # Wait at most RETRY_INTERVAl for incoming requests to the server
        # before telling the client to retry:
        self.poller.poll(RETRY_INTERVAL)
        return 'retry'
            
    def release(self, filepath, client_id):
        if filepath in self.held_locks:
            if self.held_locks[filepath]['client_id'] == client_id and self.held_locks[filepath]['expiry'] > time.time():
                del self.held_locks[filepath]
                logger.info('%s released %s'%(client_id, filepath))
                return 'ok'
        raise RuntimeError('lock timed out or was not acquired prior to release')
                        
    def run(self):
        logger.info('This is zlock server, running on port %d'%self.port)
        handlers = {'hello': self.hello, 'acquire': self.acquire, 'release': self.release}
        while True:
            # Forward a request from the router, through
            # the dealer, to the REP socket:
            message = self.router.recv_multipart()
            self.dealer.send_multipart(message)
            messages = self.sock.recv_multipart()
            # Handle the request:
            request, args = messages[0], messages[1:]
            try:
                response = handlers[request](*args)
            except Exception:
                response = traceback.format_exc()
                logger.error('%s:\n%s'%(str(messages), response))
            # Send the response back through the REP socket, then forward
            # from the dealer to the router back to the client:
            self.sock.send(response)                        
            message = self.dealer.recv_multipart()
            self.router.send_multipart(message)
            
            
if __name__ == '__main__':
    logger = setup_logging()
    server = ZMQLockServer(port)
    while True:
        try:
            server.run()
        except KeyboardInterrupt:
            logger.info('KeyboardInterrupt, stopping')
            break
        except Exception:
            message = traceback.format_exc()
            logger.critical('unhandled exception, attempting to restart:\n%s'%message)
            # Close all sockets:
            context = zmq.Context.instance()
            context.destroy(linger=False)
            # Re-initialise the server:
            server = ZMQLockServer(port)
            
            
            
    
