.. _select:

Data Retrieval
==============

Query data from already created tables. The ``SELECT`` call follows mostly the standard SQL conventions,
including all typical ingredients (such as ``WHERE``, ``GROUP BY``, ``ORDER BY`` etc.).

.. raw:: html

    <div class="highlight-sql notranslate"><div class="highlight"><pre><span></span><span class="k">SELECT</span> <span class="p">[</span> <span class="k">ALL</span> <span class="o">|</span> <span class="k">DISTINCT</span> <span class="p">]</span>
        <span class="o">*</span> <span class="o">|</span> <span class="ss">&lt;expression&gt;</span> <span class="p">[</span> <span class="p">[</span> <span class="k">AS</span> <span class="p">]</span> <span class="ss">&lt;alias&gt;</span> <span class="p">]</span> <span class="p">[</span> <span class="p">,</span> <span class="p">...</span> <span class="p">]</span>
        <span class="p">[</span> <span class="k">FROM</span> <span class="ss">&lt;from&gt;</span> <span class="p">[ ,</span> <span class="p">...</span> <span class="p">]</span> <span class="p">]</span>
        <span class="p">[</span> <span class="k">WHERE</span> <span class="ss">&lt;filter-condition&gt;</span> <span class="p">]</span>
        <span class="p">[</span> <span class="k">GROUP</span> <span class="k">BY</span> <span class="ss">&lt;group-by&gt;</span> <span class="p">]</span>
        <span class="p">[</span> <span class="k">HAVING</span> <span class="ss">&lt;having-condition&gt;</span> <span class="p">]</span>
        <span class="p">[</span> <span class="k">UNION</span> <span class="p">[</span> <span class="k">ALL</span> <span class="o">|</span> <span class="k">DISTINCT</span> <span class="p">]</span> <span class="ss">&lt;select&gt;</span> <span class="p">]</span>
        <span class="p">[</span> <span class="k">ORDER</span> <span class="k">BY</span> <span class="ss">&lt;order-by&gt;</span> <span class="p">[</span> <span class="k">ASC</span> <span class="o">|</span> <span class="k">DESC</span> <span class="p">]</span> [</span> <span class="p">,</span> <span class="p">...</span> <span class="p">] <span class="p">]</span>
        <span class="p">[</span> <span class="k">LIMIT</span> <span class="ss">&lt;end&gt;</span> <span class="p">]</span>
        <span class="p">[</span> <span class="k">OFFSET</span> <span class="ss">&lt;start&gt;</span> <span class="p">]</span>
    </pre></div>
    </div>

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

Special Operations: ``CASE``, ``NOT``, ``IS NULL``, ``IS NOT NULL``, ``IS TRUE``, ``IS NOT TRUE``, ``IS FALSE:``, ``IS NOT FALSE``, ``IS UNKNOWN``, ``IS NOT UNKNOWN``, ``EXISTS``, ``RAND``, ``RAND_INTEGER``

Example:

.. code-block:: sql

    SELECT
        SIN(x)
    FROM "data"
    WHERE MONTH(t) = 4

.. note::

    It is also possible to implement custom functions. See :ref:`custom`.

Aggregations
~~~~~~~~~~~~

``ANY_VALUE``, ``AVG``, ``BIT_AND``, ``BIT_OR``, ``BIT_XOR``, ``COUNT``, ``EVERY``, ``MAX``, ``MIN``, ``SINGLE_VALUE``, ``STDDEV_POP``, ``STDDEV_SAMP``, ``SUM``, ``VAR_POP``, ``VAR_SAMP``, ``VARIANCE``

Example:

.. code-block:: sql

    SELECT
        SUM(x)
    FROM "data"
    GROUP BY y

.. note::

    It is also possible to implement custom aggregations. See :ref:`custom`.


Table Functions
~~~~~~~~~~~~~~~

``TABLESAMPLE SYSTEM`` and ``TABLESAMPLE BERNOULLI``:

Example:

.. code-block:: sql

    SELECT * FROM "data" TABLESAMPLE BERNOULLI (20) REPEATABLE (42)

``TABLESAMPLE`` allows to draw random samples from the given table and should be the preferred way
to select samples. ``BERNOULLI`` will select a row in the original table with a probability
given by the number in the brackets (in percentage). The optional flag ``REPEATABLE`` defines
the random seed to use.
``SYSTEM`` is similar, but acts on partitions (so blocks of data) and is therefore much more
inaccurate and should only ever be used on really large data samples where ``BERNOULLI`` is not
fast enough (which is very unlikely).


