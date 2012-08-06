import os
import time
import threading
import signal
import zmq


class HeartbeatServer(object):
    """A server which recieves messages from clients and echoes them
    back. There is only one server for however many clients there are"""
    def __init__(self):
        self.running = False
        
    def run(self, context):
        if not self.running:
            self.running = True
            self.sock = context.socket(zmq.REP)
            self.port = self.sock.bind_to_random_port('tcp://127.0.0.1')
            self.mainloop_thread = threading.Thread(target=self.mainloop)
            self.mainloop_thread.daemon = True
            self.mainloop_thread.start()
        return self.port
        
    def mainloop(self):
        while True:
            msg = self.sock.recv()
            self.sock.send(msg)

class HeartbeatClient(object):
    def __init__(self,context, server_port,lock):
        self.sock = context.socket(zmq.REQ)
        self.poller = zmq.Poller()
        self.poller.register(self.sock,zmq.POLLIN)
        self.port = self.sock.connect('tcp://127.0.0.1:%s'%server_port)
        self.mainloop_thread = threading.Thread(target=self.mainloop)
        self.mainloop_thread.daemon = True
        self.mainloop_thread.start()
        self.pid = str(os.getpid())
        self.lock = lock

    def mainloop(self):
        while True:
            try:
                time.sleep(1)
                self.sock.send(self.pid, zmq.NOBLOCK)
                events = self.poller.poll(1000)
                if events:
                    msg = self.sock.recv()
                    assert msg == self.pid
                else:
                    break
            except Exception as e:
                break
        if self.lock is not None:
            with self.lock: 
                os.kill(os.getpid(), signal.SIGTERM)
        else:
            os.kill(os.getpid(), signal.SIGTERM)
