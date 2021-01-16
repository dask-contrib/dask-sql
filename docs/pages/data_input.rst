.. _data_input:

Data Loading and Input
======================

Before data can be queried with ``dask-sql``, it needs to be loaded into the dask cluster (or local instance) and registered with the :class:`dask_sql.Context`.
For this, ``dask-sql`` uses the wide field of possible `input formats  <https://docs.dask.org/en/latest/dataframe-create.html>`_ of ``dask``, plus some additional formats only suitable for `dask-sql`.
You have multiple possibilities to load input data in ``dask-sql``:

1. Load it via python
-------------------------------

You can either use already created dask dataframes or create one by using the :func:`create_table` function.
Chances are high, there exists already a function to load your favorite format or location (e.g. s3 or hdfs).
See below for all formats understood by ``dask-sql``.
Make sure to install required libraries both on the driver and worker machines.

.. code-block:: python

    import dask.dataframe as dd
    from dask_sql import Context

    c = Context()
    df = dd.read_csv("s3://nyc-tlc/trip data/yellow_tripdata_2019-01.csv")

    c.create_table("my_data", df)

or in short (equivalent):

.. code-block:: python

    from dask_sql import Context

    c = Context()

    c.create_table("my_data", "s3://nyc-tlc/trip data/yellow_tripdata_2019-01.csv")

2. Load it via SQL
------------------

If you are connected to the SQL server implementation or you do not want to issue python command calls, you can also
achieve the data loading via SQL only.

.. code-block:: sql

    CREATE TABLE my_data WITH (
        format = 'csv',
        location = 's3://nyc-tlc/trip data/yellow_tripdata_2019-01.csv'
    )

The parameters are the same as in the python function described above.
You can find more information in :ref:`creation`.

3. Persist and share data on the cluster
----------------------------------------

In ``dask``, you can publish datasets with names into the cluster memory.
This allows to reuse the same data from multiple clients/users in multiple sessions.

For example, you can publish your data using the ``client.publish_dataset`` function of the ``distributed.Client``,
and then later register it in the :class:`dask_sql.Context` via SQL:

.. code-block:: python

    # a dask.distributed Client
    client = Client(...)
    client.publish_dataset(my_ds=df)

Later in SQL:

.. code-block:: SQL

    CREATE TABLE my_data WITH (
        format = 'memory',
        location = 'my_ds'
    )

Note, that the format is set to ``memory`` and the location is the name, which was chosen when publishing the dataset.

To achieve the same thing from python, you can just use dask's methods to get the dataset

.. code-block:: python

    df = client.get_dataset("my_df")
    c.create_table("my_data", df)


Input Formats
-------------

``dask-sql`` understands (thanks to the large Dask ecosystem) a wide verity of input formats and input locations.

* All formats and locations mentioned in `the Dask docu  <https://docs.dask.org/en/latest/dataframe-create.html>`_, including csv, parquet, json.
  Just pass in the location as string (and possibly the format, e.g. "csv" if it is not clear from the file extension).
  The data can be from local disc or many remote locations (S3, hdfs, Azure Filesystem, http, Google Filesystem, ...) - just prefix the path with the matching protocol.
  Additional arguments passed to :func:`create_table` or ``CREATE TABLE`` are given to the ``read_<format>`` calls.

  Example:

  .. code-block:: python

    c.create_table("my_data", "s3://bucket-name/my-data-*.csv",
                   storage_options={'anon': True})

  .. code-block:: sql

    CREATE TABLE my_data WITH (
        format = 'csv', -- can also be omitted, as clear from the extension
        location = 's3://bucket-name/my-data-*.csv',
        storage_options = (
            anon = True
        )
    )

* If your data is already in Pandas (or Dask) DataFrames format, you can just use it as it is via the Python API
  by giving it to :ref:`create_table` directly.
* You can connect ``dask-sql`` to an `intake <https://intake.readthedocs.io/en/latest/index.html>`_ catalog and
  use the data registered there. Assuming you have an intake catalog stored in "catalog.yaml" (can also be
  the URL of an intake server), you can read in a stored table "data_table" either via Python

  .. code-block:: python

    catalog = intake.open_catalog("catalog.yaml")
    c.create_table("my_data", catalog, intake_table_name="intake_table")
    # or
    c.create_table("my_data", "catalog.yaml", format="intake", intake_table_name="intake_table")

  or via SQL:

  .. code-block:: python

    CREATE TABLE my_data WITH (
        format = 'intake',
        location = 'catalog.yaml'
    )

  The argument ``intake_table_name`` is optional and defaults to the table name in ``dask_sql``.
  With the argument ``catalog_kwargs`` you can control how the intake catalog object is created.
  Additional arguments are forwarded to the ``to_dask()`` call of intake.
* As an experimental feature, it is also possible to use data stored in the `Apache Hive <https://hive.apache.org/>`_
  metastore. For this, ``dask-sql`` will retrieve the information on the storage location and format
  from the metastore and will then register the raw data directly in the context.
  This means, no Hive data query will be issued and you might be able to see a speed improvement.

  It is both possible to use a `pyhive.hive.Cursor` or an `sqlalchemy` connection.

  Currently, this feature is only accessible via the Python API.

  .. code-block:: python

    from dask_sql import Context
    from pyhive.hive import connect
    import sqlalchemy

    c = Context()

    cursor = connect("hive-server", 10000).cursor()
    # or
    cursor = sqlalchemy.create_engine("hive://hive-server:10000").connect()

    c.create_table("my_data", cursor, hive_table_name="the_name_in_hive")

  Again, ``hive_table_name`` is optional and defaults to the table name in ``dask-sql``.
  You can also control the database used in Hive via the ``hive_schema_name```parameter.
  Additional arguments are pushed to the internally called ``read_<format>`` functions.

.. note::

    For ``dask-sql`` it does not matter how you load your data.
    In all shown cases you can then use the specified table name to query your data
    in a ``SELECT`` call.

    Please note however that un-persisted data will be reread from its source (e.g. on S3 or disk)
    on every query whereas persisted data is only read once.
    This will increase the query speed, but will also prevent you from seeing external updates to your
    data (until you reload it explicitly).