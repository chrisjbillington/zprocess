import os
import sys
import Queue
import subprocess
from multiprocessing.managers import SyncManager

def kill_on_exit(process):
    """Make a process quit when the process calling this function does"""
    path = os.path.join(os.path.dirname(__file__),'killswitch.py')
    killswitch = subprocess.Popen(['python',path,str(process.pid), str(os.getpid())])
    
def make_server_manager():
    """ Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
    """
    job_q = Queue.Queue()
    result_q = Queue.Queue()

    # This is based on the examples in the official docs of multiprocessing.
    # get_{job|result}_q return synchronized proxies for the actual Queue
    # objects.
    class JobQueueManager(SyncManager):
        pass

    JobQueueManager.register('get_job_q', callable=lambda: job_q)
    JobQueueManager.register('get_result_q', callable=lambda: result_q)
    
    auth=os.urandom(8)
    
    manager = JobQueueManager(address=('', 0), authkey=auth)
    manager.start()
    port = manager.address[1]
    return manager, port, auth
    
def make_client_manager(ip, port, auth):
    """ Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
    """
    class ServerQueueManager(SyncManager):
        pass

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(ip, port), authkey=auth)
    manager.connect()

    return manager

def subprocess_with_queues(path):
    manager, port, auth = make_server_manager()
    to_child = manager.get_job_q()
    from_child = manager.get_result_q()
    child = subprocess.Popen(['python',path,str(port),auth],stdin=subprocess.PIPE)
    kill_on_exit(child)
    return to_child, from_child, child, manager
    
def setup_connection_with_parent():
    port = int(sys.argv[1])
    auth = sys.argv[2]
    manager = make_client_manager('',port, auth)

    from_parent = manager.get_job_q()
    to_parent = manager.get_result_q()
    return to_parent, from_parent
    
