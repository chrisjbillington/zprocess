# Run this setup script like so:
# python setup.py build_ext --inplace

from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

import numpy

ext_modules = [Extension("heartbeating", ["heartbeating.pyx"])]
setup(
    name = "heartbeating",
    cmdclass = {"build_ext": build_ext},
    ext_modules = ext_modules
)
