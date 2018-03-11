#####################################################################
#                                                                   #
# process_class_wrapper.py                                          #
#                                                                   #
# Copyright 2013, Chris Billington                                  #
#                                                                   #
# This file is part of the zprocess project (see                    #
# https://bitbucket.org/cbillington/zprocess) and is licensed under #
# the Simplified BSD License. See the license.txt file in the root  #
# of the project for the full license.                              #
#                                                                   #
#####################################################################


def _setup():
    # Clear the namespace of any evidence we were here:
    del globals()['_setup']
    import sys, os

    # Ensure the zprocess we import is the same on as we are running from,
    # relevant particularly for running the test suite:
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if not parent_dir in sys.path:
        sys.path.insert(0, parent_dir)

    from zprocess import ProcessTree
    process_tree = ProcessTree.connect_to_parent(lock=True)
    module_name, module_filepath, syspath = process_tree.from_parent.get()
    # Set sys.path so that all modules imported in the user's code are
    # importable here:
    sys.path = syspath
    sys.path.append(os.path.dirname(module_filepath))
    if module_name == '__main__':
        # Execute the user's module in __main__, so that the class is
        # unpickleable. Otherwise __main__ will refer to this file, which is
        # not where their class is! Temporarily rename this module so that the
        # user's __main__ block doesn't execute:
        globals()['__name__'] = 'process_class_wrapper'
        exec(compile(open(module_filepath, "rb").read(),module_filepath, 'exec'),
             globals(), globals())
        # Set __name__ back to normal. Runtime checks of this now cannot
        # distinguish between parent and child processes, but I think wanting
        # to do so without already knowing yourself is probably poor form:
        globals()['__name__'] = '__main__'
        
    process_cls = process_tree.from_parent.get()
    instance = process_cls(process_tree)
    instance._run() 
    
if __name__ == '__main__':
    _setup()
