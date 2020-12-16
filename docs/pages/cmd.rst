.. _cmd:

Command Line Tool
=================

It is also possible to run a small CLI tool for testing out some
SQL commands quickly.

You can either call the CLI tool (after installation) directly

.. code-block:: bash

    dask-sql

or by running these lines of code

.. code-block:: python

    from dask_sql import cmd_loop

    cmd_loop()

Some options can be set, e.g. to preload some testdata.
Have a look into :func:`dask_sql.cmd_loop` or call

.. code-block:: bash

    dask-sql --help

Of course, it is also possible to call the usual ``CREATE TABLE``
commands.

Very similar as described in :ref:`server`, it is possible to preregister your own data sources
or choose a dask scheduler to connect to.