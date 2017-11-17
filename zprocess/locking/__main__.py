#####################################################################
#                                                                   #
# __main__.py                                                       #
#                                                                   #
# Copyright 2013, Chris Billington                                  #
#                                                                   #
# This file is part of the zprocess project (see                    #
# https://bitbucket.org/cbillington/zprocess) and is licensed under #
# the Simplified BSD License. See the license.txt file in the root  #
# of the project for the full license.                              #
#                                                                   #
#####################################################################
from __future__ import division, unicode_literals, print_function, absolute_import
import os
import sys
import traceback
import time
import logging, logging.handlers
import random

import zmq

import zprocess.locking as zlock


MAX_RESPONSE_TIME = 1 # sec
LOGGING = True
DEBUG = False

# Protocol description:
#
# Clients make requests as multipart zmq messages of utf-8 encoded
# bytestrings. To acquire a lock, the request should be:
#
# ['acquire', some_lock_key, client_id, timeout]
#
# where some_lock_key is a string uniquely identifying the resource that is
# being locked, client id is a string uniquely identifying who is acquiring
# the lock, and timeout is (a string representation of) how long (in seconds)
# the lock should be held for in the event that it is not released, say if the
# client process dies. So for example a request to lock access to a file might
# be:
#
# ['acquire', 'Z:\\some_folder\some_file.h5', 'hostname:process_id:thread-id', '30']
#
# The server will then block for up to MAX_RESPONSE_TIME attempting to
# acquire the lock (it continues to serve other requests in this time,
# some of which may release the lock), and responds as soon as it can
# (immediately if the lock is currently free).  If it succeeds it will
# respond with a single zmq message:
#
# ['ok']
#
# If it can't acquire it after MAX_RESPONSE_TIME, it will instead
# respond with:
#
# ['retry']
#
# The client is free to retry immediately at that point, if it is going
# to retry there is no need to insert a delay before doing so. Not having
# a delay will not create tons of network activity as this only happens
# once every MAX_RESPONSE_TIME in the case of ongoing lock contention.
#
#
# Anything else the server replies with will be a single zmq message
# and should be considered an error and raised in the client code. This
# will occur if the client provides the wrong number of messages, if it
# spells 'acquire' wrong or similar, or if there is an exception in the
# server due to a bug in the server itself. If you see a server crash,
# please tell me (chrisjbillington@gmail.com) about it.
#
# To release a lock, send a three-part multipart message:
#
# ['release', some_lock_key, client_id]
#
# so for example:
#
# ['release', 'Z:\\some_folder\some_file.h5', 'hostname:process_id:thread-id']
#
# The server will respond with:
#
# ['ok']
#
# And again anything else (always a single message though) indicates an
# exception, perhaps inicating that the client releasing the lock never
# held it in the first place, or that it had expired.
#
# You can also send ['hello'], and the server will respond with ['hello']
# to show that it is alive and working. Or you can send ['status'],
# and the server will respond with a message with information about
# currently held locks.

def setup_logging():

    if DEBUG:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    logger = logging.getLogger('ZLock')
    logger.setLevel(loglevel)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(message)s')
    if sys.stdout is not None and sys.stdout.isatty():
        terminalhandler = logging.StreamHandler(sys.stdout)
        terminalhandler.setFormatter(formatter)
        terminalhandler.setLevel(loglevel)
        logger.addHandler(terminalhandler)
    else:
        # Prevent bug on windows where writing to stdout without a command
        # window causes a crash:
        sys.stdout = sys.stderr = open(os.devnull,'w')
    if os.name == 'nt':
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'zlock.log')
    else:
        path = '/var/log/zlock.log'
    try:
        handler = logging.handlers.RotatingFileHandler(path, maxBytes=1024*1024*50, backupCount=1)
        handler.setFormatter(formatter)
        handler.setLevel(loglevel)
        logger.addHandler(handler)
    except IOError:
        logger.warning('Can\'t open or do not have permission to write to log file %s. '%path + 
                       'Only terminal logging will be output.')
    return logger


class ZMQLockServer(object):
    
    def __init__(self, port):
        self.port = int(port)
        
        # A dictionary of locks currently held by clients:
        self.held_locks = {}
        
        # We use a ROUTER instead of a REP socket so that we can delay
        # replying to some requests until after other requests have
        # been processed. This is useful in the case of lock contention:
        # instead of replying saying 'sorry, the lock is already held', we
        # can instead wait until some other requests have been processed
        # (which may release the lock) before replying. Then the client
        # doesn't have to wait in between polls, the waiting happens on
        # the server.

        self.context = zmq.Context.instance()
        self.router = self.context.socket(zmq.ROUTER)
        self.poller = zmq.Poller()
        self.poller.register(self.router, zmq.POLLIN)
        
        # Bind the router to the outside world:
        self.router.bind('tcp://0.0.0.0:%d'%self.port)
        
        self.handlers = {'hello': self.hello, 
                         'acquire': self.acquire,
                         'release': self.release,
                         'status': self.status,
                         'clear': self.clear}
        
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
    
    def status(self):
        lines = ['ok']
        fmt = lambda key, client, expiry: ('-------\n'
                                           '   key: %s\n'%key +
                                           'client: %s\n'%client+
                                           'expiry: %d'%expiry + (' [EXPIRED]' if expiry < 0 else ''))
                                           
        for key, lock in list(self.held_locks.items()):
            lines.append(fmt(key, lock['client_id'], int(lock['expiry']-time.time())))
        lines.append('-------')
        if not self.held_locks:
            lines.append('no locks currently held')
            lines.append('-------')
        response = '\n'.join(lines)
        if LOGGING: logger.info('Got a status request. Status is: %s'%response)
        return response
    
    def clear(self, clear_all=False):
        if clear_all:
            if clear_all.lower() == 'false':
                clear_all = False
        if LOGGING: logger.info('Got a request to clear %s locks'%('*all*' if clear_all else 'expired'))
        for key, lock in list(self.held_locks.copy().items()):
            if clear_all or time.time() > lock['expiry']:
                del self.held_locks[key]
        return 'ok'
        
    def handle_one_request(self, request, *args):
        try:
            response = self.handlers[request](*args)
        except Exception:
            response = traceback.format_exc()
            logger.error('%s:\n%s'%(str([request] + list(args)), response))
        return response
                          
    def run(self):
        if LOGGING: logger.info('This is zlock server, running on port %d'%self.port)
        unprocessed_messages = []
        poll_interval = -1
        while True:
            # Wait at most RETRY_INTERVAL for incoming request messages:
            events = self.poller.poll(poll_interval)
            poll_interval = -1
            if events:
                # If there was a new request, this will be processed
                # first, being prepended to unprocessed_messages.  Then we
                # will process any other requests that are waiting,
                # in case the locks they are waiting on have timed out,
                # or MAX_RESPONSE_TIME has elapse and we need to reply
                # to their client.
                new_request_message = self.router.recv_multipart()
                unprocessed_messages.insert(0, (new_request_message, time.time() + MAX_RESPONSE_TIME))
                if LOGGING: logger.debug(new_request_message)
            else:
                if LOGGING: logger.debug('processing existing requests')
            # Process all waiting request messages:
            n_requests_processed = 0
            for request_message, expiry in unprocessed_messages[:]:
                # Unpack the REQ multipart message:
                prefix, args = request_message[0:2], request_message[2:]
                decoded_args = [arg.decode('utf8', 'surrogateescape') for arg in args]
                # Handle the request:
                response = self.handle_one_request(*decoded_args)
                if response == 'retry' and expiry - time.time() > 0:
                    # Lock contention. Lock acquisition will be retried
                    # after other requests are processed, or once maximum
                    # response time is reached. Don't give the client
                    # a response yet:
                     continue
                else:
                    # If success or error, tell the client about it. Or
                    # if we have already been retrying their request for
                    # MAX_RESPONSE_TIME, forward the 'retry' response
                    # to them.
                    unprocessed_messages.remove((request_message, expiry))
                    self.router.send_multipart(prefix + [response.encode('utf8')])
                    n_requests_processed += 1
            # Shuffle the waiting requests so as to remove any systematic
            # ordering effects:
            random.shuffle(unprocessed_messages)
            if n_requests_processed > 0:
                # If at least one request was processed, then perhaps
                # a previously contended lock is now free. Process all
                # requests again immediately:
                poll_interval = 0
            elif unprocessed_messages:
                # Otherwise, when is the soonest time that a client
                # requires a response? Process the requests again then:
                poll_interval = 1000*min(t - time.time() for m, t in unprocessed_messages)
                # ensure non-negative, that would block forever:
                poll_interval = max(0, poll_interval)
            else:
                poll_interval = -1
                
if __name__ == '__main__':
    if LOGGING: logger = setup_logging()
    
    try:
        import six
        if six.PY2:
            import ConfigParser
        else:
            import configparser as ConfigParser
        from labscript_utils.labconfig import LabConfig
        port = LabConfig().get('ports','zlock')
    except (ImportError, IOError, ConfigParser.NoOptionError):
        if LOGGING: logger.warning("Couldn't get port setting from LabConfig. Using default port")
        port = zlock.DEFAULT_PORT
    
    server = ZMQLockServer(port)
    while True:
        try:
            server.run()
        except KeyboardInterrupt:
            if LOGGING: logger.info('KeyboardInterrupt, stopping')
            server.context.destroy(linger=False)
            break
        except Exception:
            message = traceback.format_exc()
            if LOGGING: logger.critical('unhandled exception, attempting to restart:\n%s'%message)
            # Close all sockets:
            server.context.destroy(linger=False)
            # Re-initialise the server:
            server = ZMQLockServer(port)
            time.sleep(1)
            
            
            
    
