.. _best_practices:

Best Practices and Performance Tips
===================================

Sort and Use Read Filtering
---------------------------

If you often read by key ranges or perform lots of logic with groups of related records, you should consider using Dask Dataframe's ``shuffle``.
This operation ensures that all rows of a given key will be within a single partition.
This is helpful for querying records on a specific key or keys such as customer IDs or session keys, as it allows Dask to skip partitions based on the partition min and max values thus avoiding reading each record.
This can save a large amount of IO time and is especially helpful when using a network file system.

For example, querying a specific pickup time from a taxi dataset ends up returning a result with over 200 partitions as each of these partitions needs to be checked for that key.

.. code-block:: python

    ddf = dd.read_parquet('/data/taxi_pq_2GB', split_row_groups=False)
    c.create_table('taxi_unsorted', ddf)
    c.sql("select * from taxi_unsorted where DAYOFMONTH(pickup_datetime) = 15").npartitions

.. code-block::

    244

But, if you were to instead sort by the pickup time and use the ``DISTRIBUTE BY`` operation, which is equivalent to Dask Dataframe's shuffle, you can reduce the number of partitions in the result to 1.

.. code-block:: python

    def intra_partition_sort(df, sort_keys):
        return df.sort_values(sort_keys)

    c.sql("""
    SELECT
        DAYOFMONTH(pickup_datetime) AS dom,
        HOUR(pickup_datetime) AS hr,
        *
    FROM
        taxi_unsorted
    DISTRIBUTE BY dom
    """).map_partitions(intra_partition_sort, ['dom', 'hr']).to_parquet('/data/taxi_sorted')

.. code-block:: python

    sorted_ddf = dd.read_parquet(
        '/data/taxi_sorted',
        split_row_groups=False,
        filters=[
            [("dom", "==", 15)]
        ]
    )

    c.create_table("taxi_sorted", sorted_ddf)
    .sql("SELECT * FROM taxi_sorted WHERE dom = 15").npartitions

.. code-block::

    1

This comes with a large corresponding boost in computation speed. For example,

.. code-block:: python

    %%time
    c.sql("SELECT COUNT(*) FROM taxi_unsorted WHERE DAYOFMONTH(pickup_datetime) = 15").compute()

.. code-block::

    CPU times: user 2.4 s, sys: 275 ms, total: 2.68 s
    Wall time: 2.58 s

.. code-block:: python

    %%time
    c.sql("SELECT COUNT(*) FROM taxi_sorted WHERE dom = 15").compute()

.. code-block::

    CPU times: user 318 ms, sys: 21.7 ms, total: 340 ms
    Wall time: 274 ms


For a deeper dive into read filtering with Dask, check out `Filtered Reading with RAPIDS & Dask to Optimize ETL <https://medium.com/rapids-ai/filtered-reading-with-rapids-dask-to-optimize-etl-5f1624f4be55>`_.

Avoid Unnecessary Parallelism
-----------------------------

Additionally, more tasks added to the Dask graph means more overhead added by the scheduler which can be
a major performance inhibitor at large scales.

For CPUs this isn't as much of an issue, as CPUs tend to have allowance for more workers and CPU tasks tend to take longer, so the additional overhead is relatively less impactful.
But, for GPUs there's typically only one worker per GPU and tasks tend to be shorter, so the overhead added by a large number of tasks can greatly affect performance.

Improve performance by only creating tasks as necessary. For example, splitting row groups creates more tasks so avoid this if possible.

.. code-block:: python

    weather_dir = '/data/weather_pq_2GB/*.parquet'


.. code-block:: sql

    CREATE OR REPLACE TABLE weather_split WITH (
        location = '{weather_dir}',
        gpu=True,
        split_row_groups=True
    )

.. code-block:: sql

    SELECT COUNT(*) FROM weather_split WHERE type='PRCP'


.. code-block:: sql

    CREATE OR REPLACE TABLE weather_nosplit WITH (
        location = '{weather_dir}',
        gpu=True,
        split_row_groups=False
    )

.. code-block:: sql

    SELECT COUNT(*) FROM weather_nosplit WHERE type='PRCP'


Predicate Pushdown
------------------

In many cases Dask-SQL can automate sorting and read filtering with its predicate pushdown support.

For example, the query

.. code-block:: sql

    SELECT
        COUNT(*)
    FROM
        taxi
    WHERE
        DAYOFMONTH(pickup_datetime) = 15

would automatically perform the same sorting and read filtering logic as the previous section.
TODO: which cases this does and doesn't work

Use broadcast joins when possible
---------------------------------

Joins and grouped aggregations typically require communication between workers, which can be expensive.
Broadcast joins can help reduce this communication in the case of joining a small table to a large table by just sending the small table to each partition of the large table.
However, in Dask-SQL this only works when the small table is a single partition.

For example, if you read in some tables and concatenate them with a ``UNION ALL`` operation

.. code-block:: sql

    CREATE OR REPLACE TABLE precip AS
    SELECT
        station_id,
        substring("date", 0, 4) as yr,
        substring("date", 5, 2) as mth,
        substring("date", 7, 2) as dy,
        val*1/10*0.0393701 as inches
    FROM weather_nosplit
    WHERE type='PRCP'

.. code-block:: sql

    CREATE OR REPLACE TABLE atlanta_stations WITH (
        location = '/data/atlanta_stations/*.parquet',
        gpu=True
    )

.. code-block:: sql

    CREATE OR REPLACE TABLE seattle_stations WITH (
        location = '/data/seattle_stations/*.parquet',
        gpu=True
    )


.. code-block:: sql

    CREATE OR REPLACE TABLE city_stations AS
    SELECT * FROM atlanta_stations
    UNION ALL
    SELECT * FROM seattle_stations

you get a new table that has two partitions. Then if you use it in a join

.. code-block:: sql

    SELECT
        yr,
        city,
        CASE WHEN city='Atlanta' THEN
            sum(inches)/{atl_stations}
        ELSE
            sum(inches)/{seat_stations}
        END AS inches
    FROM precip
    JOIN city_stations
    ON precip.station_id = city_stations.station_id
    GROUP BY yr, city
    ORDER BY yr ASC

Dask-SQL won't perform a broadcast join and will instead perform a traditional join with a corresponding slow compute time.
However, if you were to repartition the smaller table to a single partition and rerun the operation

.. code-block:: python

    c.create_table("city_stations", c.sql("select * from city_stations").repartition(npartitions=1))

.. code-block:: sql

    SELECT
        yr,
        city,
        CASE WHEN city='Atlanta' THEN
            sum(inches)/{atl_stations}
        ELSE
            sum(inches)/{seat_stations}
        END AS inches
    FROM precip
    JOIN city_stations
    ON precip.station_id = city_stations.station_id
    GROUP BY yr, city
    ORDER BY yr ASC

Dask-SQL is able to recognize this as a broadcast join and the result is a significantly faster compute time.

Optimize Partition Sizes for GPUs
---------------------------------
File formats like `Apache ORC <https://orc.apache.org/>`_ and `Apache Parquet <https://parquet.apache.org/>`_ are designed so that they can be pulled from disk and be deserialized by CPUs quickly.
However, loading data into GPUs has a substantial additional cost in the form of transfers from CPU to GPU memory.
Minimizing that cost is often achieved by increasing partition size.
Even when using Dask-SQL on GPUs, upstream CPU systems will likely produce small files resulting in small partitions.
It's worth taking the time to repartition to larger partition sizes before querying the files on GPUs, especially when querying the same files multiple times.

There's no single optimal size so choose a size that's tuned for your workflow.
Operations like joins and concatenations greatly increase GPU memory utilization, even if temporarily, but if you're not performing many of these operations, the larger the partition size the better.
Larger partition sizes increase disk to GPU throughput and keep GPU utilization higher for faster runtimes.

We recommend a starting point of around 2gb uncompressed data per partition for GPUs.
It's usually not necessary to change from default settings when running Dask-SQL on CPUs, but if you want to manually set partition sizes, we've found 128-256mb per partition to be a good starting place.
