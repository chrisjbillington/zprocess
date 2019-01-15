from __future__ import unicode_literals, print_function, division
import sys
PY2 = sys.version_info.major == 2
if PY2:
    str = unicode
import os
import unittest
import xmlrunner

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

from zprocess.remote.server import RemoteProcessServer
from zprocess.remote import RemoteProcessClient, PROTOCOL_VERSION

class ZprocessRemoteTests(unittest.TestCase):
    def setUp(self):
        # Run the server on a random port on localhost:
        self.server = RemoteProcessServer(
            bind_address='tcp://127.0.0.1',
            silent=False,
        )
        self.port = self.server.port
        self.client = RemoteProcessClient(host='localhost', port=self.port)

    def tearDown(self):
        self.server.shutdown()
        self.server = None
        self.port = None
        self.client = None

    def test_hello(self):
        result = self.client.say_hello()
        self.assertEqual(result, 'hello')

    def test_protocol(self):
        result = self.client.get_protocol()
        self.assertEqual(result, PROTOCOL_VERSION)


if __name__ == '__main__':
    output = 'test-reports'
    if PY2:
        output = output.encode('utf8')
    testRunner = xmlrunner.XMLTestRunner(output=output, verbosity=3)
    unittest.main(verbosity=3, testRunner=testRunner, exit=not sys.flags.interactive)
