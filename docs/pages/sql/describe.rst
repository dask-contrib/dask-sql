Metadata Information
====================

With these operations, it is possible to get information on the currently registered tables
and their columns.
The output format is mostly compatible with the presto format.

.. raw:: html

    <div class="highlight"><pre>
    <span class="k">SHOW SCHEMAS</span>
    <span class="k">SHOW TABLES FROM</span> <span class="ss">&lt;schema-name&gt;</span>
    <span class="k">SHOW COLUMNS FROM</span> <span class="ss">&lt;table-name></span>
    <span class="k">DESCRIBE</span> <span class="ss">&lt;table-name></span>
    <span class="k">ANALYZE TABLE</span> <span class="ss">&lt;table-name&gt;</span> <span class="k">COMPUTE STATISTICS</span>
        [ <span class="k">FOR ALL COLUMNS</span> | <span class="k">FOR COLUMNS</span> <span class="ss">&lt;column&gt;</span>, [ ,... ] ]
    </pre></div>

See :ref:`sql` for information on how to reference schemas and tables correctly.

``SHOW SCHEMAS``
----------------

Show the schemas registered in ``dask-sql``.
Only included for compatibility reasons.
There is always just a one called "schema", where all the data is located and an additional schema, called "information_schema",
which is needed by some BI tools (which is empty).

Example:

.. raw:: html

    <div class="highlight"><pre>
    <span class="k">SHOW SCHEMAS</span>
    </pre></div>

Result:

+------------------------+
| Schema                 |
+========================+
| schema                 |
+------------------------+
| information_schema     |
+------------------------+

``SHOW TABLES``
---------------

Show the registered tables in a given schema.

Example:

.. raw:: html

    <div class="highlight"><pre>
    <span class="k">SHOW TABLES FROM</span> <span class="ss">"schema"</span>
    </pre></div>

Result:

+------------+
| Table      |
+============+
| timeseries |
+------------+

``SHOW COLUMNS`` and ``DESCRIBE``
---------------------------------

Show column information on a specific table.

Example:

.. raw:: html

    <div class="highlight"><pre>
    <span class="k">SHOW COLUMNS FROM</span> <span class="ss">"timeseries"</span>
    </pre></div>

Result:

+--------+---------+---------------+
| Column |    Type | Extra Comment |
+========+=========+===============+
|     id |  bigint |               |
+--------+---------+---------------+
|   name | varchar |               |
+--------+---------+---------------+
|      x |  double |               |
+--------+---------+---------------+
|      y |  double |               |
+--------+---------+---------------+

The column "Extra Comment" is shown for compatibility with presto.


``ANALYZE TABLE``
-----------------

Calculate statistics on a given table (and the given columns or all columns)
and return it as a query result.
Please note, that this process can be time consuming on large tables.
Even though this statement is very similar to the ``ANALYZE TABLE`` statement in e.g. `Apache Spark <https://spark.apache.org/docs/3.0.0/sql-ref-syntax-aux-analyze-table.html>`_, it does not optimize subsequent queries (as the pendent in Spark will do).

Example:

.. raw:: html

    <div class="highlight"><pre>
    <span class="k">ANALYZE TABLE</span> <span class="ss">"timeseries"</span> <span class="k">COMPUTE STATISTICS</span> <span class="k">FOR COLUMNS</span> <span class="ss">x</span>, <span class="ss">y</span>
    </pre></div>

Result:

+-----------+-----------+-----------+
|           |         x |         y |
+===========+===========+===========+
| count     |        30 |        30 |
+-----------+-----------+-----------+
| mean      |  0.140374 | -0.107481 |
+-----------+-----------+-----------+
| std       |  0.568248 |  0.573106 |
+-----------+-----------+-----------+
| min       | -0.795112 | -0.966043 |
+-----------+-----------+-----------+
| 25%       | -0.379635 | -0.561234 |
+-----------+-----------+-----------+
| 50%       | 0.0104101 | -0.237795 |
+-----------+-----------+-----------+
| 75%       |   0.70208 |  0.263459 |
+-----------+-----------+-----------+
| max       |  0.990747 |  0.947069 |
+-----------+-----------+-----------+
| data_type |    double |    double |
+-----------+-----------+-----------+
| col_name  |         x |         y |
+-----------+-----------+-----------+