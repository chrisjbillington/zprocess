#!/usr/bin/env python

# To upload a version to PyPI, run:
#
#    python setup.py sdist upload
#
# If the package is not registered with PyPI yet, do so with:
#
# python setup.py register

from setuptools import setup
import os

VERSION = '2.14.0'

# Auto generate a __version__ package for the package to import
with open(os.path.join('zprocess', '__version__.py'), 'w') as f:
    f.write("__version__ = '%s'\n"%VERSION)


# Do a check for pyzmq separately from specifying dependencies to setuptools, because we
# do not want pip to install pyzmq automatically on Windows. The user should use conda
# to install it, otherwise they will have slow cryptography.
MIN_PYZMQ_VERSION = '18.0'


def zmq_version_ok():
    try:
        import zmq
    except ImportError:
        return '<none>', False
    if zmq.__version__.split('.') < MIN_PYZMQ_VERSION.split('.'):
        return zmq.__version__, False
    return zmq.__version__, True

_pyzmq_msg = """pyzmq %s found. zprocess requires pyzmq >= %s. Please install or upgrade
pyzmq prior to installing zprocess. It is strongly recommended to use conda to install
pyzmq on Windows, as the conda package is built with faster encryption."""

pyzmq_version, version_ok = zmq_version_ok()

if os.name == 'nt' and not version_ok:
    raise RuntimeError(_pyzmq_msg % (pyzmq_version, MIN_PYZMQ_VERSION))


dependencies = ['pyzmq >= %s' % MIN_PYZMQ_VERSION, 'xmlrunner']

import sys
if sys.version_info.major == 2:
    # Backported modules:
    dependencies.append('ipaddress')
    dependencies.append('subprocess32')
    dependencies.append('enum34')
if os.name == 'nt':
    # Windows-specific modules:
    dependencies.append('pywin32')
    dependencies.append('windows-curses')

setup(
    name='zprocess',
    version=VERSION,
    description="A set of utilities for multiprocessing using zeromq.",
    author='Chris Billington',
    author_email='chrisjbillington@gmail.com',
    url='https://bitbucket.org/cbillington/zprocess/',
    license="BSD",
    packages=[
        'zprocess',
        'zprocess.zlock',
        'zprocess.locking',
        'zprocess.zlog',
        'zprocess.remote',
        'zprocess.examples',
        'zprocess.socks'
    ],
    install_requires=dependencies,
)
