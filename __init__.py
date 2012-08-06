import os
this_dir = os.path.abspath(os.path.dirname(__file__))

extensions = [os.path.join(this_dir,f) for f in ['heartbeating']]

for extension in extensions:
    if not (os.path.exists(extension + '.pyd') or os.path.exists(extension + '.so')):
        cwd = os.getcwd()
        try:
            os.chdir(this_dir)
            if os.system('python setup.py build_ext --inplace'):
                raise Exception('error building Cython extensions')
            break
        finally:
            os.chdir(cwd)
            
from spawning import subprocess_with_queues, setup_connection_with_parent
