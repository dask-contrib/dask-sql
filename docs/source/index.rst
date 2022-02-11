dask-sql
========

``dask-sql`` is a distributed SQL query engine in Python.
It allows you to query and transform your data using a mixture of
common SQL operations and Python code and also scale up the calculation easily
if you need it.

* **Combine the power of Python and SQL**: load your data with Python, transform it with SQL, enhance it with Python and query it with SQL - or the other way round.
  With ``dask-sql`` you can mix the well known Python dataframe API of `pandas` and ``Dask`` with common SQL operations, to
  process your data in exactly the way that is easiest for you.
* **Infinite Scaling**: using the power of the great ``Dask`` ecosystem, your computations can scale as you need it - from your laptop to your super cluster - without changing any line of SQL code. From k8s to cloud deployments, from batch systems to YARN - if ``Dask`` `supports it <https://docs.dask.org/en/latest/setup.html>`_, so will ``dask-sql``.
* **Your data - your queries**: Use Python user-defined functions (UDFs) in SQL without any performance drawback and extend your SQL queries with the large number of Python libraries, e.g. machine learning, different complicated input formats, complex statistics.
* **Easy to install and maintain**: ``dask-sql`` is just a pip/conda install away (or a docker run if you prefer). No need for complicated cluster setups - ``dask-sql`` will run out of the box on your machine and can be easily connected to your computing cluster.
* **Use SQL from wherever you like**: ``dask-sql`` integrates with your jupyter notebook, your normal Python module or can be used as a standalone SQL server from any BI tool. It even integrates natively with `Apache Hue <https://gethue.com/>`_.
* **GPU Support**: ``dask-sql`` has `experimental` support for running SQL queries on CUDA-enabled GPUs by utilizing `RAPIDS <https://rapids.ai>`_ libraries like `cuDF <https://github.com/rapidsai/cudf>`_ , enabling accelerated compute for SQL.


Example
-------

For this example, we use some data loaded from disk and query it with a SQL command.
``dask-sql`` accepts any pandas, cuDF, or dask dataframe as input and is able to read data directly from a variety of storage formats (csv, parquet, json) and file systems (s3, hdfs, gcs):

.. tabs::

   .. group-tab:: CPU

      .. code-block:: python

         import dask.datasets
         from dask_sql import Context

         # create a context to register tables
         c = Context()

         # create a table and register it in the context
         df = dask.datasets.timeseries()
         c.create_table("timeseries", df)

         # execute a SQL query; the result is a "lazy" Dask dataframe
         result = c.sql("""
            SELECT
               name, SUM(x) as "sum"
            FROM
               timeseries
            GROUP BY
               name
         """)

         # actually compute the query...
         result.compute()

         # ...or use it for another computation
         result.sum.mean().compute()

   .. group-tab:: GPU

      .. code-block:: python

         import dask.datasets
         from dask_sql import Context

         # create a context to register tables
         c = Context()

         # create a table and register it in the context
         df = dask.datasets.timeseries()
         c.create_table("timeseries", df, gpu=True)

         # execute a SQL query; the result is a "lazy" Dask dataframe
         result = c.sql("""
            SELECT
               name, SUM(x) as "sum"
            FROM
               timeseries
            GROUP BY
               name
         """)

         # actually compute the query...
         result.compute()

         # ...or use it for another computation
         result.sum.mean().compute()


.. toctree::
   :maxdepth: 1
   :caption: Contents:

   installation
   quickstart
   sql
   data_input
   custom
   machine_learning
   api
   server
   cmd
   how_does_it_work


.. note::

   ``dask-sql`` is currently under development and does so far not understand all SQL commands.
   We are actively looking for feedback, improvements and contributors!
