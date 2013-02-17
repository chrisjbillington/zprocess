
def _setup():
    # Clear the namespace of any evidence we were here:
    del globals()['_setup']
    from subproc_utils import setup_connection_with_parent
    to_parent, from_parent, kill_lock = setup_connection_with_parent(lock=True)
    module_name, module_filepath = from_parent.get()
    # If the Process class was defined in the parent's __main__ module,
    # run the code from that file so that the class is unpickleable
    # (otherwise __main__ would refer to the file you're readin now,
    # in which the class is not defined)
    if module_name == '__main__':
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
