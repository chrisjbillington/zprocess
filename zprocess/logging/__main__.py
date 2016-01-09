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

import os
import sys
import traceback
import time
import zmq
import zprocess.logging as zlog

LOGGING = True
# The path to our own log file:
if os.name == 'nt':
    ZLOG_LOG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__),'zlog.log'))
else:
    ZLOG_LOG_PATH = '/var/log/zlog.log'
    
# Protocol description:
#
# To log, clients should connect on a zmq.PUSH socket and send a
# multipart zmq message:
#
# ['/path/to/log/file.log', 'log text']
# 
# Both the path and text should be utf-8 encoded if they include non-ascii
# chars. log_text doesn't need a trailing newline character, zlog
# will add that.
#
# For all other functions below, the client should connect on a
# zmq.REQ socket. The server is a zmq.ROUTER, so it can handle both of
# these. Avoid using a single zmq.DEALER for a client, as then errors in
# request formatting may result in replies that are not read by your code
# right away, and will be read later by different parts of your code. This
# would be hard to debug. So use a seperate zmq.PUSH for actual logging
# and zmq.REQ for other requests. Remember to set a send high water mark
# on your PUSH socket, so that if the zlog server goes down your client
# doesn't memory leak. If the HWM is reached, handle the resulting send
# error (perhaps coughing a warning) so that your program doesn't crash
# (the python client does all this, this is advice for other clients).
#
# To check if zlog is running, clients should send:
#
# ['hello'].
#
# Zlog will respond with ['hello'].
#
# To check that zlog can log ok, clients should send:
#
# ['/path/to/log/file.log']
#
# Zlog with respond ['ok'] if it can open the file in append mode. Otherwise
# it will respond with the error message it got when it tried.
#
# To configure log rotation, clients can send a multipart message:
#
# ['setrotate', '/path/to/log/file', '<maxsize>', '<maxfiles>']
#
# Zlog will confiure itself to rotate the log file if it is greater than
# <maxsize> in MB.  It will keep at most <maxfiles>, deleting older
# ones. Set <maxfiles> to 0 for no maximum. Zlog will respond with
# ['ok'] if it has the required permissions to create and rotate the
# files. Otherwise it will respond with the error message it got when
# it checked. Rotation will then be automatic.

MAX_OPEN_FILES = 500

class ZMQLogServer(object):
    
    def __init__(self, port):
        self.port = int(port)
        context = zmq.Context.instance()
        self.sock = context.socket(zmq.ROUTER)
        
        # Bind to the outside world:
        self.sock.bind('tcp://0.0.0.0:%d'%self.port)
        
        # Keep a list of opened files so we can close old ones and not
        # run out of descriptors at the operating system level:
        self.open_files = {}
        self.last_access = {}
     
        # Eat our own dogfood: Set up logging via ourself:
        self.logger = zlog.Logger(filepath=ZLOG_LOG_PATH, _skip_pre_checks=True)
        # Instantiate a log client in the client module, as it cannot do
        # so itself without calling connect(), which will deadlock waiting
        # for a response that we can't give until our mainloop is running!
        zlog._zmq_log_client = zlog.ZMQLogClient('localhost', self.port)
        # We similarly must do this manually:
        result = self.check_file_access(ZLOG_LOG_PATH)
        if result != 'ok':
            # But we want to continue, some clients are relying on
            # us...admin will just have to see stderr or notice there
            # is no log file for the server itself and fix this.
            # Just disable file logging for now.
            self.logger.terminal_only = True
            
    def get_file(self, filepath, fresh=False):
        self.last_access[filepath] = time.time()
        if not fresh:
            try:
                f = self.open_files[filepath]
                fstat, pathstat = os.fstat(f.fileno()), os.stat(filepath)
                assert fstat.st_ino == pathstat.st_ino and fstat.st_dev == pathstat.st_dev
                return f
            except (KeyError, OSError, AssertionError):
                # Either we don't have it, or it's been deleted or moved
                # or something. Better open it from scratch.
                pass
        self.open_files[filepath] = open(filepath,'a')
        # Now we check if we have too many files open, and close some:
        while len(self.open_files) > MAX_OPEN_FILES:
            oldest_file = min(self.last_access, key=self.last_access.__getitem__)
            del self.last_access[oldest_file]
            self.open_files[oldest_file].close()
            del self.open_files[oldest_file]
        return self.open_files[filepath]
        
    def check_file_access(self, filepath):
        try:
            f = self.get_file(filepath, fresh=True)
            return 'ok'
        except Exception as e:
            if LOGGING: self.logger.exception("can't access %s"%filepath)
            return str(e)
        
    def setrotate(self, filepath, maxsize, maxfiles):
        return 'ok'
        
    def checkrotate(self, filepath):
        return 'ok'
    
    def log(self, filepath, message):
        try:
            f = self.get_file(filepath)
            f.write(message+'\n')
            f.flush()
        except Exception as e:
            if filepath == ZLOG_LOG_PATH:
                # Well well. Can't log to our own log file. Better not
                # log an error about that, that would recurse. Just print
                # to stderr. Admin will simply have to notice and fix,
                # there is no way for us to tell them without raising
                # an exception and doing a sys.exit().
                self.logger.terminal_only = True
            if LOGGING: self.logger.exception("can't log to %s\n"%filepath)
            # Nothing else we can do, can't inform the client
        
    def run(self):
        if LOGGING: self.logger.info('This is zlog server, running on port %d'%self.port)
        while True:
            request = self.sock.recv_multipart()
            routing_id = request[0]
            if not request[1]:
                # An empty frame. That means it's a REQ client.
                args = request[2:]
                if LOGGING: self.logger.info(str(args))
                if args == ['hello']:
                    result = 'hello'
                elif len(args) == 1:
                    result = self.check_file_access(*args)
                elif len(args) == 4 and args[0] == 'setrotate':
                    result = self.checkrotate(*args)
                else:
                    result = 'invalid request'
                self.sock.send_multipart([routing_id, '', result])
            else:
                # No empty frame; it's a PUSH client. 
                args = request[1:]
                if len(args) == 2:
                    self.log(*args)
                
                
                
if __name__ == '__main__':
    try:
        import six
        if six.PY2:
            import ConfigParser
        else:
            import configparser as ConfigParser
        from LabConfig import LabConfig
        port = LabConfig().get('ports','zlog')
        found_config = True
    except (ImportError, IOError, ConfigParser.NoOptionError):
        found_config = False
        port = zlog.DEFAULT_PORT
    server = ZMQLogServer(port)
    if not found_config:
        if LOGGING: server.logger.warning("Couldn't get port setting from LabConfig. Using default port")
    while True:
        try:
            server.run()
        except KeyboardInterrupt:
            if LOGGING: server.logger.info('KeyboardInterrupt, stopping\n')
            break
        except Exception:
            message = traceback.format_exc()
            if LOGGING: server.logger.critical('unhandled exception, attempting to restart')
            # Close all sockets:
            context = zmq.Context.instance()
            context.destroy(linger=False)
            time.sleep(1)
            # Re-initialise the server:
            server = ZMQLogServer(port)
            
            
            
    
