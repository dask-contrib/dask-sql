dask-sql
========

A SQL Engine for dask

``dask-sql`` adds a SQL query layer on top of dask.
This allows you to query and transform your dataframes using common SQL operations and enjoy the fast and scaling processing of ``dask``.

Example
-------

We use the timeseries random data from dask.datasets as an example,
but any other data (from disk, S3, API, hdfs) can be used.

.. code-block:: python

   from dask_sql import Context
   from dask.datasets import timeseries

   # Create a context to hold the registered tables
   c = Context()

   # If you have a cluster of dask workers,
   # initialize it now

   # Load the data and register it in the context
   # This will give the table a name
   df = timeseries()
   c.create_table("timeseries", df)

   # Now execute an SQL query. The result is a dask dataframe
   # The query looks for the id with the highest x for each name
   # (this is just random test data, but you could think of looking
   # for outliers in the sensor data)
   result = c.sql("""
      SELECT
         lhs.name,
         lhs.id,
         lhs.x
      FROM
         timeseries AS lhs
      JOIN
         (
            SELECT
               name AS max_name,
               MAX(x) AS max_x
            FROM timeseries
            GROUP BY name
         ) AS rhs
      ON
         lhs.name = rhs.max_name AND
         lhs.x = rhs.max_x
   """)

   # Show the result...
   print(result.compute())

   # ... or use it for any other dask calculation
   # (just an example, could also be done via SQL)
   print(result.x.mean().compute())

The API of ``dask-sql`` is very similar to the one from `blazingsql <http://blazingsql.com/>`_,
which makes interchanging distributed CPU and GPU calculation easy.


.. toctree::
   :maxdepth: 1
   :caption: Contents:

   pages/installation
   pages/quickstart
   pages/sql
   pages/data_input
   pages/custom
   pages/machine_learning
   pages/api
   pages/server
   pages/cmd
   pages/how_does_it_work


.. note::

   ``dask-sql`` is currently under development and does so far not understand all SQL commands.
   We are actively looking for feedback, improvements and contributors!
