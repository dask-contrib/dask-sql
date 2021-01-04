.. _creation:

Table Creation
==============

As described in :ref:`quickstart`, it is possible to register an already
created dask dataframe with a call to ``c.create_table``.
However, it is also possible to load data directly from disk (or s3, hdfs, URL, hive, ...)
and register it as a table in ``dask_sql``.
Behind the scenes, a call to one of the ``read_<format>`` of the ``dask.dataframe``
will be executed.
Additionally, queries can be materialized into new tables for caching or faster access.

.. code-block:: sql

    CREATE TABLE <table-name> WITH ( <key> = <value>, ...)
    CREATE TABLE <table-name> AS ( SELECT ... )
    CREATE VIEW <table-name> AS ( SELECT ... )

See :ref:`sql` for information on how to reference tables correctly.

.. note::

    As there is only a single schema "schema" in ``dask-sql``,
    table names should not include a separator "." in ``CREATE`` calls.

``CREATE TABLE WITH``
---------------------

This will create and register a new table "df" with the data under the specified location
and format.
With the ``persist`` parameter, it can be controlled if the data should be cached
or re-read for every SQL query.
The additional parameters are passed to the call to ``read_<format>``.
If you omit the format argument, it will be deduced from the file name extension.
More ways to load data can be found in :ref:`data_input`.

Example:

.. code-block:: sql

    CREATE TABLE df WITH (
        location = "/some/file/path",
        format = "csv/parquet/json/...",
        persist = True,
        additional_parameter = value,
        ...
    )

``CREATE TABLE AS``
-------------------

Using a similar syntax, it is also possible to create a (materialized) view of a (maybe complicated) SQL query.
With the command, you give the result of the ``SELECT`` query a name, that you can use
in subsequent calls.

Example:

.. code-block:: sql

    CREATE TABLE my_table AS (
        SELECT
            a, b, SUM(c)
        FROM data
        GROUP BY a, b
        ...
    )

    SELECT * FROM my_table

``CREATE VIEW AS``
------------------

Instead of using ``CREATE TABLE`` it is also possible to use ``CREATE VIEW``.
The result is very similar, the only difference is, *when* the result will be computed: a view is recomputed on every usage,
whereas a table is only calculated once on creation (also known as a materialized view).
This means, if you e.g. read data from a remote file and the file changes, a query containing a view will
be updated whereas a query with a table will stay as it is.
To update a table, you need to recreate it.

.. hint::

    Use views to simplify complicated queries (like a "shortcut") and tables for caching.

.. note::

    The update of the view only works, if your primary data source (the files you were reading in),
    are not persisted during reading.

Example:

.. code-block:: sql

    CREATE VIEW my_table AS (
        SELECT
            a, b, SUM(c)
        FROM data
        GROUP BY a, b
        ...
    )

    SELECT * FROM my_table