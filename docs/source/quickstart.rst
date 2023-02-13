.. _quickstart:

Quickstart
==========

After :ref:`installation`, you can start querying your data using SQL.

Run the following code in an interactive Python session, a Python script or a Jupyter Notebook.

0. Cluster Setup
----------------

If you just want to try out ``dask-sql`` quickly, this step can be skipped.
However, the real magic of ``dask`` (and ``dask-sql``) comes from the ability to scale the computations over multiple cores and/or machines.
For local development and testing, a Distributed ``LocalCluster`` (or, if using GPUs, a `Dask-CUDA <https://docs.rapids.ai/api/dask-cuda/nightly/index.html>`_ ``LocalCUDACluster``) can be deployed and a client connected to it like so:

..
    TODO - Incorrectly formatted
.. tabs::

    .. group-tab:: CPU

        .. code-block:: python

            from distributed import Client, LocalCluster

            cluster = LocalCluster()
            client = Client(cluster)

    .. group-tab:: GPU

        .. code-block:: python

            from dask_cuda import LocalCUDACluster
            from distributed import Client

            cluster = LocalCUDACluster()
            client = Client(cluster)

There are several options for deploying clusters depending on the platform being used and the resources available; see `Dask - Deploying Clusters <https://docs.dask.org/en/latest/deploying.html>`_ for more information.

1. Data Loading
---------------

Before querying the data, you need to create a ``dask`` `data frame <https://docs.dask.org/en/latest/dataframe.html>`_ containing the data.
``dask`` understands many different `input formats <https://docs.dask.org/en/latest/dataframe-create.html>`_ and sources.
In this example, we do not read in external data, but use test data in the form of random event time series:

.. code-block:: python

    import dask.datasets

    df = dask.datasets.timeseries()

Read more on the data input part in :ref:`data_input`.

2. Data Registration
--------------------

If we want to work with the data in SQL, we need to give the data frame a unique name.
We do this by registering the data in an instance of a :class:`~dask_sql.Context`:

..
    TODO - Incorrectly formatted
.. tabs::

    .. group-tab:: CPU

        .. code-block:: python

            from dask_sql import Context

            c = Context()
            c.create_table("timeseries", df)

    .. group-tab:: GPU

        .. code-block:: python

            from dask_sql import Context

            c = Context()
            c.create_table("timeseries", df, gpu=True)

From now on, the data is accessible as the ``timeseries`` table of this context.
It is possible to register multiple data frames in the same context.

..
    TODO - Incorrectly formatted
.. hint::

    If you plan to query the same data multiple times,
    it might make sense to persist the data before:

    .. tabs::

        .. group-tab:: CPU

            .. code-block:: python

                c.create_table("timeseries", df, persist=True)

        .. group-tab:: GPU

            .. code-block:: python

                c.create_table("timeseries", df, persist=True, gpu=True)

3. Run your queries
-------------------

Now you can go ahead and query the data with normal SQL!

.. code-block:: python

    result = c.sql("""
        SELECT
            name, SUM(x) AS "sum"
        FROM timeseries
        WHERE x > 0.5
        GROUP BY name
    """)
    result.compute()

``dask-sql`` understands a large fraction of SQL commands, but there are still some missing.
Have a look into the :ref:`sql` description for more information.

If you are using ``dask-sql`` from a Jupyter notebook, you might be interested in the ``sql`` magic function:

.. code-block:: python

    c.ipython_magic()

    %%sql
    SELECT
        name, SUM(x) AS "sum"
    FROM timeseries
    WHERE x > 0.5
    GROUP BY name

..
    TODO - Incorrectly formatted
.. note::

    If you have found an SQL feature, which is currently not supported by ``dask-sql``,
    please raise an issue on our `issue tracker <https://github.com/dask-contrib/dask-sql/issues>`_.
