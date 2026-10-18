# zprocess

A set of utilities for multiprocessing using
zeromq. Includes process creation and management, output
redirection, message passing, inter-process locks, logging,
and a process-tree-wide event system.

( 
[view on pypi](https://pypi.python.org/pypi/zprocess/);
[view on Github](https://github.com/chrisjbillington/zprocess)
)

   * Install `python setup.py install`.

## Security

Communication across the network is encrypted and authenticated with CurveZMQ using a
shared secret, generated with `python -m zprocess.makesecret`. This requires a libzmq
built with CURVE support. The pyzmq package on the Anaconda default channel for Windows
is built without it; the pyzmq packages on conda-forge and on PyPI support CURVE. Without
CURVE support zprocess raises `zprocess.security.CurveNotSupported` when a shared secret
is generated or used.
