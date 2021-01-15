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

.. raw:: html

    <div class="highlight-sql notranslate">
    <div class="highlight"><pre>
    <span class="k">CREATE</span> [ <span class="k">OR REPLACE</span> ] <span class="k">TABLE</span> [ <span class="k">IF NOT EXISTS</span> ] <span class="ss">&lt;table-name></span>
        <span class="k">WITH</span> ( <span class="ss">&lt;key&gt;</span> = <span class="ss">&lt;value&gt;</span> [ , ... ] )
    <span class="k">CREATE</span> [ <span class="k">OR REPLACE</span> ] <span class="k">TABLE</span> [ <span class="k">IF NOT EXISTS</span> ] <span class="ss">&lt;table-name></span>
        <span class="k">AS</span> ( <span class="k">SELECT</span> ... )
    <span class="k">CREATE</span> [ <span class="k">OR REPLACE</span> ] <span class="k">VIEW</span> [ <span class="k">IF NOT EXISTS</span> ] <span class="ss">&lt;table-name></span>
        <span class="k">AS</span> ( <span class="k">SELECT</span> ... )
    <span class="k">DROP TABLE</span> | <span class="k">VIEW</span> [ <span class="k">IF EXISTS</span> ] <span class="ss">&lt;table-name></span>
    </pre></div>
    </div>

See :ref:`sql` for information on how to reference tables correctly.
Please note, that there can only ever exist a single view or table with the same name.

.. note::

    As there is only a single schema "schema" in ``dask-sql``,
    table names should not include a separator "." in ``CREATE`` calls.

By default, if a table with the same name does already exist, ``dask-sql`` will raise an exception
(and in turn will raise an exception if you try to delete a table which is not present).
With the flags ``IF [NOT] EXISTS`` and ``OR REPLACE``, this behavior can be controlled:

* ``CREATE OR REPLACE TABLE | VIEW`` will override an already present table/view with the same name without raising an exception.
* ``CREATE TABLE IF NOT EXISTS`` will not create the table/view if it already exists (and will also not raise an exception).
* ``DROP TABLE | VIEW IF EXISTS`` will only drop the table/view if it exists and will not do anything otherwise.

``CREATE TABLE WITH``
---------------------

This will create and register a new table "df" with the data under the specified location
and format.
For information on how to specify key-value arguments properly, see :ref:`sql`.
With the ``persist`` parameter, it can be controlled if the data should be cached
or re-read for every SQL query.
The additional parameters are passed to the particular data loading functions.
If you omit the format argument, it will be deduced from the file name extension.
More ways to load data can be found in :ref:`data_input`.

Example:

.. raw:: html

    <div class="highlight-sql notranslate"><div class="highlight"><pre><span></span><span class="k">CREATE</span> <span class="k">TABLE</span> <span class="n">df</span> <span class="k">WITH</span> <span class="p">(</span>
        <span class="n">location</span> <span class="o">=</span> <span class="ss">"/some/file/path"</span><span class="p">,</span>
        <span class="n">format</span> <span class="o">=</span> <span class="ss">"csv/parquet/json/..."</span><span class="p">,</span>
        <span class="n">persist</span> <span class="o">=</span> <span class="k">True</span><span class="p">,</span>
        <span class="n">additional_parameter</span> <span class="o">=</span> <span class="n">value</span><span class="p">,</span>
        <span class="p">...</span>
    <span class="p">)</span>
    </pre></div>
    </div>

``CREATE TABLE AS``
-------------------

Using a similar syntax, it is also possible to create a (materialized) view of a (maybe complicated) SQL query.
With the command, you give the result of the ``SELECT`` query a name, that you can use
in subsequent calls.
The ``SELECT`` can also contain a call to ``PREDICT``, see :ref:`ml`.

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

``DROP TABLE | VIEW``
---------------------

Remove a table or view with the given name.
Please note again, that views and tables are treated equally, so ``CREATE TABLE``
will also delete the view with the given name and vise versa.