# Backward compatibility submodule, will be removed in zprocess 3

import sys
import os

# Ensure zprocess is in the path if we are running from this directory
if os.path.abspath(os.getcwd()) == os.path.dirname(os.path.abspath(__file__)):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.getcwd())))

from zprocess.zlock.__main__ import main
if __name__ == '__main__':
    main()