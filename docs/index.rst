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

For this example, we use some data loaded from disk and query them with a SQL command from our python code.
Any pandas or dask dataframe can be used as input and ``dask-sql`` understands a large amount of formats (csv, parquet, json,...) and locations (s3, hdfs, gcs,...).

.. code-block:: python

   import dask.dataframe as dd
   from dask_sql import Context

   # Create a context to hold the registered tables
   c = Context()

   # Load the data and register it in the context
   # This will give the table a name, that we can use in queries
   df = dd.read_csv("...")
   c.create_table("my_data", df)

   # Now execute a SQL query. The result is again dask dataframe.
   result = c.sql("""
      SELECT
         my_data.name,
         SUM(my_data.x)
      FROM
         my_data
      GROUP BY
         my_data.name
   """)

   # Show the result
   print(result)

   # Show the result...
   print(result.compute())

   # ... or use it for any other dask calculation
   print(result.x.mean().compute())


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
   pages/configuration


.. note::

   ``dask-sql`` is currently under development and does so far not understand all SQL commands.
   We are actively looking for feedback, improvements and contributors!
