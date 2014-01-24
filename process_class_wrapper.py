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
    from zprocess import setup_connection_with_parent
    to_parent, from_parent, kill_lock = setup_connection_with_parent(lock=True)
    module_name, module_filepath, syspath = from_parent.get()
    # Set sys.path so that all modules imported in the user's code are importable here:
    sys.path = syspath
    sys.path.append(os.path.dirname(module_filepath))
    if module_name == '__main__':
        # Execute the user's module in __main__, so that the class is unpickleable.
        # Otherwise __main__ will refer to this file, which is not where their class is!
        # Temporarily rename this module so that the user's __main__ block doesn't execute:
        globals()['__name__'] = 'process_class_wrapper'
        execfile(module_filepath, globals(), globals())
        # Set __name__ back to normal. Runtime checks of this now cannot
        # distinguish between parent and child processes, but I think
        # wanting to do so without already knowing yourself is probably
        # poor form:
        globals()['__name__'] = '__main__'
        
    process_cls = from_parent.get()
    instance = process_cls(instantiation_is_in_subprocess=True)
    instance._run(to_parent, from_parent, kill_lock) 
    
if __name__ == '__main__':
    _setup()
