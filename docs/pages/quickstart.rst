.. _quickstart:

Quickstart
==========

After :ref:`installation`, you can start querying your data using SQL.

Run the following code in an interactive python session, a python script or a jupyter notebook.

0. Cluster Setup
----------------

If you just want to try out ``dask-sql`` quickly, you can skip this step at first.
However, the real magic of ``dask`` (and ``dask-sql``) comes from the ability to scale the computations over multiple machines.
There are `plenty <https://docs.dask.org/en/latest/setup.html>`_ of possibilities to setup a ``dask`` cluster.
For local development and testing, you can setup a distributed version of ``dask`` with

.. code-block:: python

    from dask.distributed import Client

    client = Client()

1. Data Loading
---------------

Before querying the data, you need to create a ``dask`` `data frame <https://docs.dask.org/en/latest/dataframe.html>`_ containing the data.
``dask`` understands many different `input formats <https://docs.dask.org/en/latest/dataframe-create.html>`_ and sources.

In this example, we do not read in external data, but use test data in the form of random event time series.

.. code-block:: python

    import dask.datasets

    df = dask.datasets.timeseries()

Read more on the data input part in :ref:`data_input`.

2. Data Registration
--------------------

If we want to work with the data in SQL, we need to give the data frame a unique name.
We do this by registering the data at an instance of a :class:`dask_sql.Context`.
Typically, you only have a single context per application.

.. code-block:: python

    from dask_sql import Context

    c = Context()
    c.create_table("timeseries", df)

From now on, the data is accessible as the "timeseries" table of this context.
It is possible to register multiple data frames at the same context.

.. hint::

    If you plan to query the same data multiple times,
    it might make sense to persist the data before:

    .. code-block:: python

        df = df.persist()
        c.create_table("timeseries", df)


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

.. note::

    If you have found an SQL feature, which is currently not supported by ``dask-sql``,
    please raise an issue on our `issue tracker <https://github.com/nils-braun/dask-sql/issues>`_.

