.. _sql:

SQL Syntax
==========

``dask-sql`` understands SQL in (mostly) presto SQL syntax.
So far, not every valid SQL operator and keyword is already
implemented in ``dask-sql``, but a large fraction of it.
Have a look into our `issue tracker <https://github.com/nils-braun/dask-sql/issues>`_
to find out what is still missing.

``dask-sql`` understands queries for data retrieval (``SELECT``), queries on metadata information (``SHOW`` and ``DESCRIBE``), queries for table creation (``CREATE TABLE``) and machine learning (``CREATE MODEL`` and ``PREDICT``).
In the following, general information for these queries are given - the sub-pages give details on each of the implemented keywords or operations.
The information on these pages apply to all ways SQL queries can be handed over to ``dask-sql``: via Python (:ref:`api`), the SQL server (:ref:`server`) or the command line (:ref:`cmd`).

General
-------

Data in ``dask-sql`` is - similar to most SQL systems - grouped in named tables, which consist of columns (with names and data types) and rows.
The tables are again grouped into schemas.
For simplicity, there only exists a single schema, named "schema".

For many queries, it is necessary to refer to a schema, table, or column.
Identifiers can be specified with double quotes or without quotes (if there is no ambiguity with SQL keywords).
Casing will be kept (with or without quotes).

.. code-block:: sql

    SELECT
        "date", "name"
    FROM
        "df"

``"date"`` definitely needs quotation marks (as ``DATE`` is also an SQL keyword), but ``name`` and ``df`` can also be specified without quotation marks.

To prevent ambiguities, the full table identifier can be used:

.. code-block:: sql

    SELECT
        "df"."date"
    FROM
        "schema"."df"

In many cases however, the bare name is enough:

.. code-block:: sql

    SELECT
        "date"
    FROM
        "df"

String literals get single quotes:

.. code-block:: sql

    SELECT 'string literal'

.. note::

    ``dask-sql`` can only understand a single SQL query per call to ``Context.sql``.
    Therefore, there should also be no semicolons after the query.


Some SQL statements, like ``CREATE MODEL WITH`` and ``CREATE TABLE WITH`` expect a list of key-value arguments,
which resemble (not accidentally) a Python dictionary. They are in the form

.. code-block:: none

    (
        key = value
        [ , ... ]
    )

with an arbitrary number of key-value pairs and always are enclosed in brackets. The keys are (similar to Pythons ``dict`` constructor) unquoted.
A value can be any valid SQL literal (e.g. ``3``, ``4.2``, ``'string'``), a key-value parameter list itself
or a list (``ARRAY``) or a set (``MULTISET``) (or another way of writing dictionaries with ``MAP``).

This means, the following is a valid key-value parameter list:

.. code-block:: sql

    (
        first_argument = 3,
        second_argument = MULTISET [ 1, 1, 2, 3 ],
        third_argument = (
            sub_argument_1 = ARRAY [ 1, 2, 3 ],
            sub_argument_2 = 'a string'
        )
    )

Please note that, in contrast to python, no comma is allowed after the last argument.

Query Types and Reference
-------------------------

.. toctree::
   :maxdepth: 1

   sql/select.rst
   sql/creation.rst
   sql/ml.rst
   sql/describe.rst


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

* Not all operations and aggregations are implemented already, most prominently: ``WINDOW`` is not implemented so far.
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
