.. _sql:

SQL Syntax
==========

``dask-sql`` understands SQL in postgreSQL syntax.
So far, not every valid postgreSQL operator and keyword is already
implemented in ``dask-sql``, but a large fraction of it.
Have a look into our `issue tracker <https://github.com/nils-braun/dask-sql/issues>`_
to find out what is still missing.

General
-------

Identifiers can be specified with double quotes or without quotes (if there is no ambiguity with SQL keywords).

.. code-block:: sql

    SELECT
        "date", "name"
    FROM
        "df"

``"date"`` definitely needs quotation marks (as ``DATE`` is also an SQL keyword), but ``name`` and ``df`` can also be specified without quotation marks.
To prevent ambiguities, the full table identifier can be used - but in many cases the bare column name is enough.

String literals get single quotes:

.. code-block:: sql

    SELECT 'string literal'

``dask-sql`` can only understand a single SQL query per call to ``Context.sql``.
Therefore, there should also be no semicolons after the query.

Selecting
---------

The typical ingredients of a ``SELECT`` are also possible in ``dask-sql``:

.. code-block:: sql

    SELECT
        name, SUM(x) AS s
    FROM
        data
    WHERE
        y < 3 AND x > 0.5
    GROUP BY
        name
    HAVING
        SUM(x) < 5
    UNION SELECT
        'myself' AS name, 42 AS s
    ORDER BY
        s
    LIMIT 100

Also (all kind of) joins and (complex) subqueries are possible:

.. code-block:: sql

    SELECT
         lhs.name, lhs.id, lhs.x
      FROM
         date AS lhs
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

Describing
----------

It is possible to get information on the currently registered tables
and their columns:

To show the schemas (there is always just a single one called "schema"):

.. code-block:: sql

    SHOW SCHEMAS

To show the registered tables:

.. code-block:: sql

    SHOW TABLES FROM "schema"

To show column information on a specific table named "df"

.. code-block:: sql

    SHOW COLUMNS FROM "df"

.. _creation:

Table Creation
--------------

As described in :ref:`quickstart`, it is possible to register an already
created dask dataframe with a call to ``c.create_table``.
However, it is also possible to load data directly from disk (or s3, hdfs, URL, ...)
and register it as a table in ``dask_sql``.
Behind the scenes, a call to one of the ``read_<format>`` of the ``dask.dataframe``
will be executed.

.. code-block:: sql

    CREATE TABLE df WITH (
        location = "/some/file/path",
        format = "csv/parquet/json/...",
        persist = True,
        additional_parameter = value,
        ...
    )

This will create and register a new table "df" with the data under the specified location
and format.
With the ``persist`` parameter, it can be controlled if the data should be cached
or re-read for every SQL query.
The additional parameters are passed to the call to ``read_<format>``.
If you omit the format argument, it will be deduced from the file name extension.
More ways to load data can be found in :ref:`data_input`.

Using a similar syntax, it is also possible to create a (materialized) view of a (maybe complicated) SQL query.
With the following command, you give the result of the ``SELECT`` query a name, that you can use
in subsequent calls.

.. code-block:: sql

    CREATE TABLE my_table AS (
        SELECT
            a, b, SUM(c)
        FROM data
        GROUP BY a, b
        ...
    )

    SELECT * FROM my_table

Instead of using ``CREATE TABLE`` it is also possible to use ``CREATE VIEW``.
The result is very similar, the only difference is when the result will be computed: a view is recomputed on every usage,
whereas a table is only calculated once on creation (also known as a materialized view).
This means, if you e.g. read data from a remote file and the file changes, a query containing a view will
be updated whereas a query with a table will stay constant.
To update a table, you need to recreate it.

.. hint::

    Use views to simplify complicated queries (like a "shortcut") and tables for caching.

.. note::

    The update of the view only works, if your primary data source (the files you were reading in),
    are not persisted during reading.


Implemented operations
----------------------

The following list includes all operations understood and implemented in ``dask-sql``.
Scalar functions can be used to turn a column (or multiple) into a column of the same length (such as ``x + y`` or ``sin(x)``)
whereas aggregation functions can only be used in ``GROUP BY`` clauses, as they
turn a column into a single value.
For more information on the semantic of the different functions, please have a look into the
`Apache Calcite documentation <https://calcite.apache.org/docs/reference.html>`_.

Scalar Functions
~~~~~~~~~~~~~~~~

Binary Operations: ``AND``, ``OR``, ``>``, ``>=``, ``<``, ``<=``, ``=``, ``<>``, ``+``, ``-``, ``/``, ``*``

Unary Math Operations: ``ABS``, ``ACOS``, ``ASIN``, ``ATAN``, ``ATAN2``, ``CBRT``, ``CEIL``, ``COS``, ``COT``, ``DEGREES``, ``EXP``, ``FLOOR``, ``LOG10``, ``LN``, ``POWER``, ``RADIANS``, ``ROUND``, ``SIGN``, ``SIN``, ``TAN``, ``TRUNCATE``

String operations: ``LIKE``, ``SIMILAR TO``, ``||``, ``CHAR_LENGTH``, ``UPPER``, ``LOWER``, ``POSITION``, ``TRIM``, ``OVERLAY``, ``SUBSTRING``, ``INITCAP``

Date operations: ``EXTRACT``, ``YEAR``, ``QUARTER``, ``MONTH``, ``WEEK``, ``DAYOFYEAR``, ``DAYOFMONTH``, ``DAYOFWEEK``, ``HOUR``, ``MINUTE``, ``SECOND``, ``LOCALTIME``, ``LOCALTIMESTAMP``, ``CURRENT_TIME``, ``CURRENT_DATE``, ``CURRENT_TIMESTAMP``

.. note::

Due to a `bug/inconsistency <https://issues.apache.org/jira/browse/CALCITE-4313>`_ in Apache Calcite, both the ``CURRENTTIME`` and ``LOCALTIME`` return a time without timezone and are therefore the same functionality.

Special Operations: ``CASE``, ``NOT``, ``IS NULL``, ``IS NOT NULL``, ``IS TRUE``, ``IS NOT TRUE``, ``IS FALSE:``, ``IS NOT FALSE``, ``IS UNKNOWN``, ``IS NOT UNKNOWN``, ``EXISTS``

Aggregations
~~~~~~~~~~~~

``ANY_VALUE``, ``AVG``, ``BIT_AND``, ``BIT_OR``, ``BIT_XOR``, ``COUNT``, ``EVERY``, ``MAX``, ``MIN``, ``SINGLE_VALUE``, ``SUM``

Implemented Types
-----------------

``dask-sql`` needs to map between SQL and ``dask`` (python) types.
For this, it uses the following mapping:

+-----------------------+----------------+
| From Python Type      | To SQL Type    |
+=======================+================+
| ``np.bool8``          |  ``BOOLEAN``   |
+-----------------------+----------------+
| ``np.datetime64``     |  ``TIMESTAMP`` |
+-----------------------+----------------+
| ``np.float32``        |  ``FLOAT``     |
+-----------------------+----------------+
| ``np.float64``        |  ``DOUBLE``    |
+-----------------------+----------------+
| ``np.int16``          |  ``SMALLINT``  |
+-----------------------+----------------+
| ``np.int32``          |  ``INTEGER``   |
+-----------------------+----------------+
| ``np.int64``          |  ``BIGINT``    |
+-----------------------+----------------+
| ``np.int8``           |  ``TINYINT``   |
+-----------------------+----------------+
| ``np.object_``        |  ``VARCHAR``   |
+-----------------------+----------------+
| ``np.uint16``         |  ``SMALLINT``  |
+-----------------------+----------------+
| ``np.uint32``         |  ``INTEGER``   |
+-----------------------+----------------+
| ``np.uint64``         |  ``BIGINT``    |
+-----------------------+----------------+
| ``np.uint8``          |  ``TINYINT``   |
+-----------------------+----------------+
| ``pd.BooleanDtype``   |  ``BOOLEAN``   |
+-----------------------+----------------+
| ``pd.Int16Dtype``     |  ``SMALLINT``  |
+-----------------------+----------------+
| ``pd.Int32Dtype``     |  ``INTEGER``   |
+-----------------------+----------------+
| ``pd.Int64Dtype``     |  ``BIGINT``    |
+-----------------------+----------------+
| ``pd.Int8Dtype``      |  ``TINYINT``   |
+-----------------------+----------------+
| ``pd.StringDtype``    |  ``VARCHAR``   |
+-----------------------+----------------+
| ``pd.UInt16Dtype``    |  ``SMALLINT``  |
+-----------------------+----------------+
| ``pd.UInt32Dtype``    |  ``INTEGER``   |
+-----------------------+----------------+
| ``pd.UInt64Dtype``    |  ``BIGINT``    |
+-----------------------+----------------+
| ``pd.UInt8Dtype``     |  ``TINYINT``   |
+-----------------------+----------------+

+-------------------+-----------------------------+
| From SQL Type     | To Python Type              |
+===================+=============================+
| ``BIGINT``        |    ``pd.Int64Dtype``        |
+-------------------+-----------------------------+
| ``BOOLEAN``       |    ``pd.BooleanDtype``      |
+-------------------+-----------------------------+
| ``CHAR(*)``       |    ``pd.StringDtype``       |
+-------------------+-----------------------------+
| ``DATE``          |    ``np.dtype("<M8[ns]")``  |
+-------------------+-----------------------------+
| ``DECIMAL(*)``    |    ``np.float64``           |
+-------------------+-----------------------------+
| ``DOUBLE``        |    ``np.float64``           |
+-------------------+-----------------------------+
| ``FLOAT``         |    ``np.float32``           |
+-------------------+-----------------------------+
| ``INTEGER``       |    ``pd.Int32Dtype()``      |
+-------------------+-----------------------------+
| ``INTERVAL``      |    ``np.dtype("<m8[ns]")``  |
+-------------------+-----------------------------+
| ``SMALLINT``      |    ``pd.Int16Dtype()``      |
+-------------------+-----------------------------+
| ``TIME(*)``       |    ``np.dtype("<M8[ns]")``  |
+-------------------+-----------------------------+
| ``TIMESTAMP(*)``  |    ``np.dtype("<M8[ns]")``  |
+-------------------+-----------------------------+
| ``TINYINT``       |    ``pd.Int8Dtype``         |
+-------------------+-----------------------------+
| ``VARCHAR``       |    ``pd.StringDtype``       |
+-------------------+-----------------------------+
| ``VARCHAR(*)``    |    ``pd.StringDtype``       |
+-------------------+-----------------------------+


Limitatons
----------

``dask-sql`` is still in early development, therefore exist some limitations:

* Not all operations and aggregations are implemented already
* The first sorting direction in the ``ORDER BY`` must be in ascending order.
  All subsequent columns can also be sorted in decreasing order.
* ``GROUP BY`` aggregations can not use ``DISTINCT``

.. note::

    Whenever you find a not already implemented operation, keyword
    or functionality, please raise an issue at our `issue tracker <https://github.com/nils-braun/dask-sql/issues>`_ with your use-case.

Apart from those functional limitations, there is a operation which need special care: ``ORDER BY```.
Normally, ``dask-sql`` calls create a ``dask`` data frame, which gets only computed when you call the ``.compute()`` member.
Due to internal constraints, this is currently not the case for ``ORDER BY``.
Including this operation will trigger a calculation of the full data frame already when calling ``Context.sql()``.

.. warning::

    There is a subtle but important difference between adding ``LIMIT 10`` to your SQL query and calling ``sql(...).head(10)``.
    The data inside ``dask`` is partitioned, to distribute it over the cluster.
    ``head`` will only return the first N elements from the first partition - even if N is larger than the partition size.
    As a benefit, calling ``.head(N)`` is typically faster than calculating the full data sample with ``.compute()``.
    ``LIMIT`` on the other hand will always return the first N elements - no matter on how many partitions they are scattered - but will also need to precalculate the first partition to find out, if it needs to have a look into all data or not.
