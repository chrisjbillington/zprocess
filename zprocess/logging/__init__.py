#####################################################################
#                                                                   #
# __init__.py                                                       #
#                                                                   #
# Copyright 2013, Chris Billington                                  #
#                                                                   #
# This file is part of the zprocess project (see                    #
# https://bitbucket.org/cbillington/zprocess) and is licensed under #
# the Simplified BSD License. See the license.txt file in the root  #
# of the project for the full license.                              #
#                                                                   #
#####################################################################

from __future__ import absolute_import
import sys
import os
import socket
import threading
import time
import logging
import subprocess
import zmq

import six

DEFAULT_PORT = 7341
import __main__

class ZMQLogClient(object):

    RESPONSE_TIMEOUT = 5000
    
    def __init__(self, host, port):
        self.host = socket.gethostbyname(host)
        self.port = port
        self.lock = threading.Lock()
        # We'll store one zmq socket/poller for each thread, with thread
        # local storage:
        self.local = threading.local()
        self.shutdown_complete = threading.Event()
         
    def new_req_socket(self):
        # Every time we get an error, we need to create a new req socket
        # to get things back on track. Also, we have a separate socket for
        # each thread so that these methods can be called simultaneously
        # from many threads:
        context = zmq.Context.instance()
        self.local.reqsock = context.socket(zmq.REQ)
        self.local.reqsock.setsockopt(zmq.LINGER, 0)
        self.local.poller = zmq.Poller()
        self.local.poller.register(self.local.reqsock, zmq.POLLIN)
        self.local.reqsock.connect('tcp://%s:%s'%(self.host, str(self.port)))
         
    def new_push_socket(self):
        # We have a separate socket for each thread so that these methods
        # can be called simultaneously from many threads:
        context = zmq.Context.instance()
        self.local.pushsock = context.socket(zmq.PUSH)
        self.local.pushsock.setsockopt(zmq.LINGER, 1000)
        try:
            self.local.pushsock.setsockopt(zmq.SNDHWM, 1000)
        except AttributeError:
            self.local.pushsock.setsockopt(zmq.HWM, 1000)
        self.local.pushsock.connect('tcp://%s:%s'%(self.host, str(self.port)))
        self.local.high_water_mark = False
        
    def say_hello(self,timeout=None):
        """Ping the server to test for a response"""
        try:
            if timeout is None:
                timeout = self.RESPONSE_TIMEOUT
            else:
                timeout = 1000*timeout # convert to ms
            if not hasattr(self.local,'reqsock'):
                self.new_req_socket()
            start_time = time.time()
            self.local.reqsock.send('hello',zmq.NOBLOCK)
            events = self.local.poller.poll(timeout)
            if events:
                response = self.local.reqsock.recv()
                if response == 'hello':
                    return round((time.time() - start_time)*1000,2)
            raise zmq.ZMQError('No response from zlog server: timed out')
        except:
            self.local.reqsock.close(linger=False)
            del self.local.reqsock
            raise
    
    def log(self, logfile, logtext):
        if not hasattr(self.local,'pushsock'):
            self.new_push_socket()
        try:
            self.local.pushsock.send_multipart([logfile, logtext], zmq.NOBLOCK)
            self.local.high_water_mark = False
        except zmq.error.Again: 
            if not self.local.high_water_mark:
                self.local.high_water_mark = True
                sys.stderr.write('warning: logging server appears to be offline - messages dropped.')
            pass
    
    def check_file_access(self, filepath, timeout=None):
        """Ask the server if it can open the given file in append mode for logging"""
        try:
            if timeout is None:
                timeout = self.RESPONSE_TIMEOUT
            else:
                timeout = 1000*timeout # convert to ms
            if not hasattr(self.local,'reqsock'):
                self.new_req_socket()
            self.local.reqsock.send(filepath,zmq.NOBLOCK)
            events = self.local.poller.poll(timeout)
            if events:
                response = self.local.reqsock.recv()
                if response == 'ok':
                    return True
                else:
                    raise IOError('zlog server reported an error:\n ' +  response)
            raise zmq.ZMQError('No response from zlog server: timed out')
        except:
            self.local.reqsock.close(linger=False)
            del self.local.reqsock
            raise
            
    def setrotate(self, *args):
        raise NotImplementedError
        

def check_connected():
    try:
        _zmq_log_client
    except NameError:
        raise RuntimeError('Not connected to a zlog server. Call zlog.connect()')
                
def ping(timeout=None):
    check_connected()
    return _zmq_log_client.say_hello(timeout)
        
def log(logfile, logtext):
    check_connected()
    return _zmq_log_client.log(logfile, logtext)
    
def check_file_access(filepath, timeout=None):
    check_connected()
    return _zmq_log_client.check_file_access(filepath, timeout)


class Formatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        t = time.strftime("%Y-%m-%d %H:%M:%S", ct)
        s = "%s,%06d" % (t, int((record.created % 1)*1e6))
        return s


class Logger(logging.Logger): 

    instances = {}
    
    def __new__(cls, name='', *args, **kwargs):
        # Get names of parent loggers:
        namelevels = name.split('.')
        parent_levels = ['.'.join(namelevels[:i+1]) for i in range(len(namelevels))]
        # Convert all args to kwargs for easier parsing:
        kwargs.update(dict(list(zip(six.get_function_code(cls.__init__).co_varnames[2:], args))))
        for parent_level in reversed(parent_levels):
            try:
                parent_logger = cls.instances[parent_level]
                break
            except KeyError:
                pass
        else:
            parent_logger = None
        # Inherit unset args from parent logger:
        new_kwargs = {}
        for argname in six.get_function_code(cls.__init__).co_varnames[2:]:
            try:
                new_kwargs[argname] = kwargs[argname]
            except KeyError:
                if parent_logger is not None:
                    try:
                        new_kwargs[argname] = parent_logger.kwargs[argname]
                    except KeyError:
                        pass
        
        instance = object.__new__(cls)
        instance.kwargs = new_kwargs
        cls.instances[name] = instance
        return instance
        
    def __init__(self, name='', filepath=None, fmt=None, to_terminal=logging.INFO, 
                 rotate_at_size_MB=None, max_rotated_files=None, terminal_only = False, _skip_pre_checks=False):
        # The __new__ method has replaced any unspecified arguments (other
        # than name) with those of the parent logger, if one was found. It
        # put the results in self.kwargs. So we will use values there
        # preferentially, then fall back on defaults.
        self.name = name
        filepath = self.kwargs.get('filepath', filepath)
        fmt = self.kwargs.get('fmt', fmt)
        to_terminal = self.kwargs.get('to_terminal', to_terminal)
        rotate_at_size_MB = self.kwargs.get('rotate_at_size_MB', rotate_at_size_MB)
        max_rotated_files = self.kwargs.get('max_rotated_files', max_rotated_files)
        terminal_only = self.kwargs.get('terminal_only', terminal_only)
        _skip_pre_checks = self.kwargs.get('_skip_pre_checks', _skip_pre_checks)
        logging.Logger.__init__(self, name)
        
        self.terminal_only = terminal_only
        self.to_terminal = to_terminal
        
        if filepath is None:
            # Absolute import means this is not zlock.__main__, this is
            # the user's __main__:
            import __main__
            try:
                main_file = __main__.__file__
            except AttributeError:
                filepath = 'interactive.log'
            else:
                main_dir, main_base = os.path.split(os.path.abspath(main_file))
                filename = os.path.splitext(main_base)[0]
                if filename in ['__init__', '__main__', 'main']:
                    # name it after the folder instead:
                    filename = os.path.split(main_dir)[1]
                filepath = os.path.join(main_dir, filename+'.log')
        filepath = os.path.abspath(filepath)
        self.filepath = filepath
        
        if not _skip_pre_checks:
            # This can be skipped, which is necessary for the zlog
            # server itself, as it cannot wait for a response from itself
            # (it is single threaded). It will do these checks manually.
            try:
                check_connected()
            except RuntimeError:
                connect()
            check_file_access(filepath, timeout=None)
        
        if fmt is None:
            if name:
                fmt = '[%(asctime)s] %(levelname)s %(name)s: %(message)s'
            else:
                fmt = '[%(asctime)s] %(levelname)s %(message)s'
                
        self.formatter = Formatter(fmt)

        if to_terminal is not None and sys.stdout is not None and sys.stdout.isatty():
            terminalhandler = logging.StreamHandler(sys.stdout)
            terminalhandler.setFormatter(self.formatter)
            terminalhandler.setLevel(to_terminal)
            self.addHandler(terminalhandler)
        elif os.name == 'nt' and not sys.stdout.isatty():
            # Prevent bug on windows where writing to stdout without a command
            # window causes a crash:
            sys.stdout = sys.stderr = open(os.devnull,'w')

    def handle(self, record):
        if self.to_terminal is not None:
            logging.Logger.handle(self, record)
        if not self.terminal_only:
            msg = self.formatter.format(record)
            log(self.filepath, msg)
            
    
def connect(host=None, port=None, timeout=None):
    """This method should be called at program startup, it establishes
    communication with the server and ensures it is responding"""
    if host is None or port is None:
        try:
            if six.PY2:
                import ConfigParser
            else:
                import configparser as ConfigParser
            from LabConfig import LabConfig
            config = LabConfig()
        except (ImportError, IOError):
            pass
        else:
            if host is None:
                try:
                    host = config.get('servers','zlog')
                except ConfigParser.NoOptionError:
                    pass
            if port is None:
                try:
                    port = config.get('ports','zlog')
                except ConfigParser.NoOptionError:
                    pass
        if host is None:
            host = 'localhost'
        if port is None:
            port = DEFAULT_PORT
    global _zmq_log_client
    _zmq_log_client = ZMQLogClient(host, port)
    if socket.gethostbyname(host) == socket.gethostbyname('localhost'):
        try:
            # short connection timeout if localhost, don't want to
            # waste time:
            ping(timeout=0.05)
        except zmq.ZMQError:
            # No zlog server running on localhost. Start one. It will run
            # forever, even after this program exits. This is important for
            # other programs which might be using it. I don't really consider
            # this bad practice since the server is typically supposed to
            # be running all the time:
            devnull = open(os.devnull,'w')
            subprocess.Popen([sys.executable,'-m','zlog'], stdout=devnull, stderr=devnull)
            # Try again. Longer timeout this time, give it time to start up:
            ping(timeout=15)
    else:
        ping(timeout)
        # We ping twice since the first does initialisation and so takes
        # longer. The second will be more accurate:
    return ping(timeout)


