.. _data_input:

Data Loading and Input
======================

Before data can be queried with ``dask-sql``, it needs to be loaded into the dask cluster (or local instance) and registered with the :class:`dask_sql.Context`.
For this, ``dask-sql`` uses the wide field of possible `input formats  <https://docs.dask.org/en/latest/dataframe-create.html>`_ of ``dask``.
You have multiple possibilities to load input data in ``dask-sql``:

1. Load it with dask via python
-------------------------------

You can either use already created dask dataframes or create one by using one of the ``read_<format>`` functions from ``dask``.
Chances are high, there exists already a function to load your favorite format or location (e.g. s3 or hdfs.
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

Load hive data
''''''''''''''

As an experimental feature, it is now also possible to use data stored in the Apache Hive metastore.
For this, ``dask-sql`` will retrieve the information on the storage location and format
from the metastore and will then register the raw data directly in the context.
This means, no Hive data query will be issued and you might be able to see a speed improvement.

.. code-block:: python

    from dask_sql import Context
    from pyhive.hive import connect

    c = Context()

    cursor = connect("hive-server", 10000).cursor()
    c.create_table("my_data", cursor, hive_table_name="the_name_in_hive")


2. Load it via SQL
------------------

If you are connected to the SQL server implementation or you do not want to issue python command calls, you can also
achieve the data loading via SQL only.

.. code-block:: sql

    CREATE TABLE my_data WITH (
        format = 'csv',
        location = 's3://nyc-tlc/trip data/yellow_tripdata_2019-01.csv'
    )

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


.. note::

    For ``dask-sql`` it does not matter how you load your data.
    In all shown cases you can then use the specified table name to query your data
    in a ``SELECT`` call.

    Please note however that un-persisted data will be reread from its source (e.g. on S3 or disk)
    on every query whereas persisted data is only read once.
    This will increase the query speed, but will also prevent you from seeing external updates to your
    data (until you reload it explicitly).