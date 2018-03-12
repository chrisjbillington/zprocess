
========================
zprocess |release|
========================

`Chris Billington <mailto:chrisjbillington@gmail.com>`_, |today|


.. contents::
    :local:

TODO: Summary

`View on PyPI <http://pypi.python.org/pypi/zprocess>`_
| `View on BitBucket <https://bitbucket.org/cbillington/zprocess>`_
| `Read the docs <http://zprocess.readthedocs.org>`_

------------
Installation
------------

to install ``zprocess``, run:

.. code-block:: bash

    $ pip3 install zprocess

or to install from source:

.. code-block:: bash

    $ python3 setup.py install

.. note::
    Also works with Python 2.7

------------
Introduction
------------

TODO: introduction


-------------
Example usage
-------------

.. code-block:: python
    :name: example.py

    def todo():
        print('example')
    todo()

.. code-block:: bash
    :name: output

    $ python3 example.py
    example

Description of examples

----------------
Module reference
----------------


.. autoclass:: zprocess.clientserver.ZMQServer
    :members:

.. autoclass:: zprocess.clientserver.ZMQClient
    :members:

.. autofunction:: zprocess.utils.start_daemon

