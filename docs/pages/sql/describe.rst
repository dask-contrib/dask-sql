Metadata Information
====================

With these operations, it is possible to get information on the currently registered tables
and their columns:

.. code-block:: sql

    SHOW SCHEMAS
    SHOW TABLES FROM "schema-name"
    SHOW COLUMNS FROM "table-name"
    DESCRIBE "table-name"

See :ref:`sql` for information on how to reference schemas and tables correctly.

``SHOW SCHEMAS``
----------------

Show the schemas registered in ``dask-sql``.
Only included for compatibility reasons.
There is always just a one called "schema", where all the data is located and an additional schema, called "information_schema",
which is needed by some BI tools (which is empty).

Example:

.. code-block:: sql

    SHOW SCHEMAS

Result:

.. code-block:: none

                   Schema
    0              schema
    1  information_schema

``SHOW TABLES``
---------------

Show the registered tables in a given schema.

Example:

.. code-block:: sql

    SHOW TABLES FROM "schema"

Result:

.. code-block:: none

            Table
    0  timeseries


``SHOW COLUMNS`` and ``DESCRIBE``
---------------------------------

Show column information on a specific table.

Example:

.. code-block:: sql

    SHOW COLUMNS FROM "timeseries"

Result:

.. code-block:: none

      Column     Type Extra Comment
    0     id   bigint
    1   name  varchar
    2      x   double
    3      y   double