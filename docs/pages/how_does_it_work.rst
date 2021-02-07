How does it work?
=================

At the core, ``dask-sql`` does two things:

- translate the SQL query using `Apache Calcite <https://calcite.apache.org/>`_ into a relational algebra,
  which is specified as a tree of java objects - similar to many other SQL engines (Hive, Flink, ...)
- convert this description of the query from java objects into dask API calls (and execute them) - returning a dask dataframe.

Th following example explains this in quite some technical details.
For most of the users, this level of technical understanding is not needed.

1. SQL enters the library
-------------------------

No matter of via the Python API (:ref:`api`), the command line client (:ref:`cmd`) or the server (:ref:`server`), eventually the SQL statement by the user will end up as a string in the function :func:`~dask_sql.Context.sql`.

2. SQL is parsed
----------------

This function will first give the SQL string to the implemented Java classes (especially :class:`RelationalAlgebraGenerator`) via the ``jpype`` library.
Inside this class, Apache Calcite is used to first parse the SQL string and then turn it into a relational algebra.
For this, Apache Calcite uses the SQL language description specified in the Calcite library itself and the additional definitions in the ``.ftl```files in the ``dask-sql`` repository.
They specify custom language features, such as the ``CREATE MODEL`` statement.

.. note::

    ``.ftl`` stands for FreeMarker Template Language and is one of the standard templating languages used in the Java ecosystem.
    Each of the "functions" defined in the documents defines a part of the (extended) SQL language in ``javacc`` format.
    FreeMarker is used to combine these parser definitions with the ones from Apache Calcite. Have a look into the ``config.fmpp`` file for more information.

    For example the following ``javacc`` code

    .. code-block::

        SqlNode SqlShowTables() :
        {
            final Span s;
            final SqlIdentifier schema;
        }
        {
            <SHOW> { s = span(); } <TABLES> <FROM>
            schema = CompoundIdentifier()
            {
                return new SqlShowTables(s.end(this), schema);
            }
        }

    describes a parser line, which understands SQL statements such as

    .. code-block:: sql

        SHOW TABLES FROM "schema"

    While parsing the SQL, they are turned into an instance of the Java class :class:`SqlShowTables` (which is also defined in this project).
    The :class:`Span` is used internally in Apache Calcite to store the position in the parsed SQL statement (e.g. for better error output).
    The ``SqlShowTables`` javacc function (not the Java class SqlShowTables) is listed in ``config.fmpp`` as a ``statementParserMethods``, which makes it parsable as main SQL statement (similar to any normal ``SELECT ...`` statement).
    All Java classes used as parser return values inherit from the Calcite class :class:`SqlNode` or any derived subclass (if it makes sense). Those classes are barely containers to store the information from the parsed SQL statements (such as the schema name in the example above) and do not have any business logic by themselves.

3. SQL is (maybe) optimized
---------------------------

Once the SQL string is parsed into an instance of a :class:`SqlNode` (or a subclass of it), Apache Calcite can convert it into a relational algebra and optimize it. As this is only implemented for Calcite-own classes (and not for the custom classes such as :class:`SqlCreateModel`) this conversion and optimization is not triggered for all SQL statements (have a look into :func:`Context._get_ral`).

After optimization, the resulting Java instance will be a class of any of the :class:`Logical*` classes in Apache Calcite (such as :class:`LogicalJoin`). Each of those can contain other instances as "inputs" creating a tree of different steps in the SQL statement (see below for an example).

So after all, the result is either an optimized tree of steps in the relational algebra (represented by instances of the :class:`Logical*` classes) or an instance of a :class:`SqlNode` (sub)class.

4. Translation to Dask API calls
--------------------------------

Depending on which type the resulting java class has, they are converted into calls to python functions using different python "converters". For each Java class, there exist a converter class in the ``dask_sql.physical.rel`` folder, which are registered at the :class:`dask_sql.physical.rel.convert.RelConverter` class.
Their job is to use the information stored in the java class instances and turn it into calls to python functions (see the example below for more information).

As many SQL statements contain calculations using literals and/or columns, these are split into their own functionality (``dask_sql.physical.rex``) following a similar plugin-based converter system.
Have a look into the specific classes to understand how the conversion of a specific SQL language feature is implemented.

5. Result
---------

The result of each of the conversions is a :class:`dask.DataFrame`, which is given to the user. In case of the command line tool or the SQL server, it is evaluated immediately - otherwise it can be used for further calculations by the user.

Example
-------

Let's walk through the steps above using the example SQL statement

.. code-block:: sql

    SELECT x + y FROM timeseries WHERE x > 0

assuming the table "timeseries" is already registered.
If you want to follow along with the steps outlined in the following, start the command line tool in debug mode

.. code-block:: bash

    dask-sql --load-test-data --startup --log-level DEBUG

and enter the SQL statement above.

First, the SQL is parsed by Apache Calcite and (as it is not a custom statement) transformed into a tree of relational algebra objects.

.. code-block:: none

    LogicalProject(EXPR$0=[+($3, $4)])
        LogicalFilter(condition=[>($3, 0)])
            LogicalTableScan(table=[[schema, timeseries]])

The tree output above means, that the outer instance (:class:`LogicalProject`) needs as input the output of the previous instance (:class:`LogicalFilter`) etc.

Therefore the conversion to python API calls is called recursively (depth-first). First, the :class:`LogicalTableScan` is converted using the :class:`rel.logical.table_scan.LogicalTableScanPlugin` plugin. It will just get the correct :class:`dask.DataFrame` from the dictionary of already registered tables of the context.
Next, the :class:`LogicalFilter` (having the dataframe as input), is converted via the :class:`rel.logical.filter.LogicalFilterPlugin`.
The filter expression ``>($3, 0)`` is converted into ``df["x"] > 0`` using a combination of REX plugins (have a look into the debug output to learn more) and applied to the dataframe.
The resulting dataframe is then passed to the converter :class:`rel.logical.project.LogicalProjectPlugin` for the :class:`LogicalProject`.
This will calculate the expression ``df["x"] + df["y"]`` (after having converted it via the class:`RexCallPlugin` plugin) and return the final result to the user.

.. code-block:: python

    df_table_scan = context.tables["timeseries"]
    df_filter = df_table_scan[df_table_scan["x"] > 0]
    df_project = df_filter.assign(col=df_filter["x"] + df_filter["y"])
    return df_project[["col"]]