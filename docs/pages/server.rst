.. _server:

SQL Server
==========

``dask-sql`` comes with a small test implementation for a SQL server.
Instead of rebuilding a full ODBC driver, we re-use the `presto wire protocol <https://github.com/prestodb/presto/wiki/HTTP-Protocol>`_.

.. note::

    It is - so far - only a start of the development and missing important concepts, such as
    authentication.

You can test the sql presto server by running (after installation)

.. code-block:: bash

    dask-sql-server

or by running these lines of code

.. code-block:: python

    from dask_sql import run_server

    run_server()

or by using the created docker image

.. code-block:: bash

    docker run --rm -it -p 8080:8080 nbraun/dask-sql

This will spin up a server on port 8080 (by default).
The port and bind interfaces can be controlled with the ``--port`` and ``--host`` command line arguments (or options to :func:`dask_sql.run_server`).

The running server looks similar to a normal presto database to any presto client and can therefore be used
with any library, e.g. the `presto CLI client <https://prestosql.io/docs/current/installation/cli.html>`_ or
``sqlalchemy`` via the `PyHive <https://github.com/dropbox/PyHive#sqlalchemy>`_ package:

.. code-block:: bash

    presto --server localhost:8080

Now you can fire simple SQL queries (as no data is loaded by default):

.. code-block::

    => SELECT 1 + 1;
     EXPR$0
    --------
        2
    (1 row)

Or via ``sqlalchemy`` (after having installed ``PyHive``):

.. code-block:: python

    from sqlalchemy.engine import create_engine
    engine = create_engine('presto://localhost:8080/')

    import pandas as pd
    pd.read_sql_query("SELECT 1 + 1", con=engine)

Of course, it is also possible to call the usual ``CREATE TABLE``
commands.

Preregister your own data sources
---------------------------------

The python function :func:`dask_sql.run_server` accepts an already created :class:`dask_sql.Context`.
This means you can preload your data sources and register them with a context before starting your server.
By this, your server will already have data to query:

.. code-block:: python

    from dask_sql import Context
    c = Context()
    c.create_table(...)

    # Then spin up the ``dask-sql`` server
    from dask_sql import run_server
    run_server(context=c)


Run it in your own ``dask`` cluster
-----------------------------------

The SQL server implementation in ``dask-sql`` allows you to run a SQL server as a service connected to your ``dask`` cluster.
This enables your users to run SQL command leveraging the full power of your ``dask`` cluster without the need to write python code
and allows also the usage of different non-python tools (such as BI tools) as long as they can speak the presto protocol.

To run a standalone SQL server in your ``dask`` cluster, follow these three steps:

1. Create a startup script to connect ``dask-sql`` to your cluster.
   There exist many different ways to connect to a ``dask`` cluster (e.g. direct access to the scheduler,
   dask gateway, ...). Choose the one suitable for your cluster and create a small startup script:

   .. code-block:: python

        # Connect to your cluster here, e.g.
        from dask.distributed import Client
        client = Client(scheduler_address)

        ...

        # Then spin up the ``dask-sql`` server
        from dask_sql import run_server
        run_server(client=client)

2. Deploy this script to your cluster as a service. How you do this, depends on your cluster infrastructure (kubernetes, mesos, openshift, ...).
   For example you could create a docker image with a dockerfile similar to this:

   .. code-block:: dockerfile

        FROM nbraun/dask-sql

        COPY startup_script.py /opt/dask_sql/startup_script.py

        ENTRYPOINT [ "/opt/conda/bin/python", "/opt/dask_sql/startup_script.py" ]

3. After your service is deployed, you can use it in your applications as a "normal" presto database.

The ``dask-sql`` SQL server was successfully tested with `Apache Hue <https://gethue.com/>`_, `Apache Superset <https://superset.apache.org/>`_
and `Metabase <https://www.metabase.com/>`_.