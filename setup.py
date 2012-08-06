# Run this setup script like so:
# python setup.py build_ext --inplace

import os

from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

import zmq.core

zmq_dir = os.path.abspath(os.path.dirname(zmq.__file__))
includes = [os.path.join(zmq_dir,sub) for sub in ['utils', 'core','devices','']]
ext_modules = [Extension("heartbeating", ["heartbeating.pyx"], include_dirs = includes)]
setup(
    name = "heartbeating",
    cmdclass = {"build_ext": build_ext},
    ext_modules = ext_modules
)
