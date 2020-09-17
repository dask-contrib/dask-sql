.. _sql:

SQL Syntax
==========

``dask-sql`` understands SQL in postgreSQL syntax.
So far, not every valid postgreSQL operator and keyword is already
implemented in ``dask-sql``.

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

Also joins and (complex) subqueries are possible:

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

Apart from those functional limitations, there are also two operations which need special care: ``ORDER BY`` and ``LIMIT``.
Normally, ``dask-sql`` calls create a ``dask`` data frame, which gets only computed when you call the ``.compute()`` member.
Due to internal constraints, this is currently not the case for ``ORDER BY`` and ``LIMIT``.
Including one of those operations will trigger a calculation of the full data frame already when calling ``Context.sql()``.

.. warning::

    There is a subtle but important difference between adding ``LIMIT 10`` to your SQL query and calling ``sql(...).head(10)``.
    The data inside ``dask`` is partitioned, to distribute it over the cluster.
    ``head`` will only return the first N elements from the first partition - even if N is larger than the partition size.
    As a benefit, calling ``.head(N)`` is typically faster than calculating the full data sample with ``.compute()``.
    ``LIMIT`` on the other hand will always return the first N elements - no matter on how many partitions they are scattered - but will also need to compute the full data set for this.