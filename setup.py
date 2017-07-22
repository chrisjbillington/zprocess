#!/usr/bin/env python

# To upload a version to PyPI, run:
#
#    python setup.py sdist upload
#
# If the package is not registered with PyPI yet, do so with:
#
# python setup.py register

from distutils.core import setup
import os

VERSION = '2.2.1'

# Auto generate a __version__ package for the package to import
with open(os.path.join('zprocess', '__version__.py'), 'w') as f:
    f.write("__version__ = '%s'\n"%VERSION)

setup(name='zprocess',
      version=VERSION,
      description="A set of utilities for multiprocessing using zeromq.",
      author='Chris Billington',
      author_email='chrisjbillington@gmail.com',
      url='https://bitbucket.org/cbillington/zprocess/',
      license="BSD",
      packages=['zprocess', 'zprocess.locking', 'zprocess.logging']
     )
