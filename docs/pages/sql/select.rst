Data Retrieval
==============

Query data from already created tables. The ``SELECT`` call follows mostly the standard SQL conventions,
including all typical ingredients (such as ``WHERE``, ``GROUP BY``, ``ORDER BY`` etc.).

.. code-block:: sql

    SELECT [ ALL | DISTINCT ]
        * | <expression> [ [ AS ] <alias> ] [ , ... ]
        [ FROM <from> [, ... ] ]
        [ WHERE <filter-condition> ]
        [ GROUP BY <group-by> ]
        [ HAVING <having-condition> ]
        [ UNION [ ALL | DISTINCT ] <select> ]
        [ ORDER BY <order-by> [ ASC | DESC ] , ... ]
        [ LIMIT <end> ]
        [ OFFSET <start> ]

.. note::

    ``WINDOW`` is currently not supported.
    If you would like to help, please see [our issue tracker](https://github.com/nils-braun/dask-sql/issues/43).

Example:

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
         data AS lhs
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

For complex queries with many subqueries, it might be beneficial to use ``WITH``
for temporary table definitions:

.. code-block:: sql

    WITH tmp AS (
        SELECT MAX(b) AS maxb from df GROUP BY a
    )
    SELECT
        maxb
    FROM tmp

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

.. note::

    It is also possible to implement custom functionality. See :ref:`custom`.