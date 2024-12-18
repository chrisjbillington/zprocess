from zprocess import Process, ProcessTree
import os

class Foo(Process):
    def run(self, data):
        print('this is a running foo in process', os.getpid())
        print('data is', data)
        message = self.from_parent.get()
        print('foo, got a message:', message)
        self.to_parent.put('hello yourself!')

# This __main__ check is important to stop the same code executing again in the child:
if __name__ == '__main__':

    process_tree = ProcessTree()
    foo = Foo(process_tree)
    to_child, from_child = foo.start('bar')
    to_child.put('hello, foo!')
    response = from_child.get()
    print('parent, got a response:', response)
