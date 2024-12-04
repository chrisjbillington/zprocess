import sys
from pathlib import Path
import unittest
import pytest

THIS_DIR = Path(__file__).absolute().parent

# Add project root to import path
PROJECT_ROOT = THIS_DIR.parent
if PROJECT_ROOT not in [Path(s).absolute() for s in sys.path]:
    sys.path.insert(0, str(PROJECT_ROOT))

from zprocess.remote.server import RemoteProcessServer
from zprocess.remote import RemoteProcessClient, PROTOCOL_VERSION
from zprocess.process_tree import ProcessTree, Process

class TestProcess(Process):
    def run(self):
        item = self.from_parent.get()
        self.to_parent.put(item)


class ZprocessRemoteTests(unittest.TestCase):
    def setUp(self):
        # Run the server on a random port on localhost:
        self.server = RemoteProcessServer(bind_address='tcp://127.0.0.1', silent=True)
        self.port = self.server.port
        self.client = RemoteProcessClient(host='localhost', port=self.port)
        self.process_tree = ProcessTree()

    def tearDown(self):
        self.server.shutdown()
        self.server = None
        self.port = None
        self.client = None
        self.process_tree = None

    def test_hello(self):
        result = self.client.say_hello()
        self.assertEqual(result, 'hello')

    def test_protocol(self):
        result = self.client.get_protocol()
        self.assertEqual(result, PROTOCOL_VERSION)

    def test_basic_process(self):
        proc = Process(
            self.process_tree,
            remote_process_client=self.client,
            # Give by import path relative to zprocess project directory where tests are running from
            subclass_fullname='tests.test_remote.TestProcess',
        )
        to_child, from_child = proc.start()
        to_child.put('hello')
        self.assertEqual(from_child.get(), 'hello')

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
