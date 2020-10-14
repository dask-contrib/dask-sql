from typing import Callable, List, Tuple
from collections import namedtuple
import logging

import dask.dataframe as dd

from dask_sql.java import (
    DaskAggregateFunction,
    DaskScalarFunction,
    DaskSchema,
    DaskTable,
    RelationalAlgebraGenerator,
    SqlParseException,
    ValidationException,
    get_java_class,
)
from dask_sql.mappings import python_to_sql_type
from dask_sql.physical.rel import RelConverter, logical, custom
from dask_sql.physical.rex import RexConverter, core
from dask_sql.datacontainer import DataContainer, ColumnContainer
from dask_sql.utils import ParsingException

logger = logging.getLogger(__name__)

FunctionDescription = namedtuple(
    "FunctionDescription", ["f", "parameters", "return_type", "aggregation"]
)


class Context:
    """
    Main object to communicate with ``dask_sql``.
    It holds a store of all registered data frames (= tables)
    and can convert SQL queries to dask data frames.
    The tables in these queries are referenced by the name,
    which is given when registering a dask dataframe.

    Example:
        .. code-block:: python

            from dask_sql import Context
            c = Context()

            # Register a table
            c.register_dask_table(df, "my_table")

            # Now execute an SQL query. The result is a dask dataframe
            result = c.sql("SELECT a, b FROM my_table")

            # Trigger the computation (or use the data frame for something else)
            result.compute()

    Usually, you will only ever have a single context in your program.

    See also:
        :func:`sql`
        :func:`register_dask_table`

    """

    def __init__(self):
        """
        Create a new context.
        """
        # Storage for the registered tables
        self.tables = {}
        # Storage for the registered functions
        self.functions = {}
        # Storage for the registered aggregations
        self.aggregations = {}
        # Name of the root schema (not changable so far)
        self.schema_name = "schema"

        # Register any default plugins, if nothing was registered before.
        RelConverter.add_plugin_class(logical.LogicalAggregatePlugin, replace=False)
        RelConverter.add_plugin_class(logical.LogicalFilterPlugin, replace=False)
        RelConverter.add_plugin_class(logical.LogicalJoinPlugin, replace=False)
        RelConverter.add_plugin_class(logical.LogicalProjectPlugin, replace=False)
        RelConverter.add_plugin_class(logical.LogicalSortPlugin, replace=False)
        RelConverter.add_plugin_class(logical.LogicalTableScanPlugin, replace=False)
        RelConverter.add_plugin_class(logical.LogicalUnionPlugin, replace=False)
        RelConverter.add_plugin_class(logical.LogicalValuesPlugin, replace=False)
        RelConverter.add_plugin_class(custom.CreateTablePlugin, replace=False)
        RelConverter.add_plugin_class(custom.ShowColumnsPlugin, replace=False)
        RelConverter.add_plugin_class(custom.ShowSchemasPlugin, replace=False)
        RelConverter.add_plugin_class(custom.ShowTablesPlugin, replace=False)

        RexConverter.add_plugin_class(core.RexCallPlugin, replace=False)
        RexConverter.add_plugin_class(core.RexInputRefPlugin, replace=False)
        RexConverter.add_plugin_class(core.RexLiteralPlugin, replace=False)

    def register_dask_table(self, df: dd.DataFrame, name: str):
        """
        Registering a dask table makes it usable in SQL queries.
        The name you give here can be used as table name in the SQL later.

        Please note, that the table is stored as it is now.
        If you change the table later, you need to re-register.

        Example:
            This code registers a data frame as table "data"
            and then uses it in a query.

            .. code-block:: python

                c.register_dask_table(df, "data")
                df_result = c.sql("SELECT a, b FROM data")

        Args:
            df (:class:`dask.dataframe.DataFrame`): The data frame to register
            name: (:obj:`str`): Under which name should the new table be addressable

        """
        self.tables[name.lower()] = DataContainer(
            df.copy(), ColumnContainer(df.columns)
        )

    def register_function(
        self,
        f: Callable,
        name: str,
        parameters: List[Tuple[str, type]],
        return_type: type,
    ):
        """
        Register a custom function with the given name.
        The function can be used (with this name)
        in every SQL queries from now on - but only for scalar operations
        (no aggregations).
        This means, if you register a function "f", you can now call

        .. code-block:: sql

            SELECT f(x)
            FROM df

        Please note that you can always only have one function with the same name;
        no matter if it is an aggregation or scalar function.

        For the registration, you need to supply both the
        list of parameter and parameter types as well as the
        return type. Use `numpy dtypes <https://numpy.org/doc/stable/reference/arrays.dtypes.html>`_ if possible.

        More information: :ref:`custom`

        Example:
            This example registers a function "f", which
            calculates the square of an integer and applies
            it to the column ``x``.

            .. code-block:: python

                def f(x):
                    return x ** 2

                c.register_function(f, "f", [("x", np.int64)], np.int64)

                sql = "SELECT f(x) FROM df"
                df_result = c.sql(sql)

        Args:
            f (:obj:`Callable`): The function to register
            name (:obj:`str`): Under which name should the new function be addressable in SQL
            parameters (:obj:`List[Tuple[str, type]]`): A list ot tuples of parameter name and parameter type.
                Use `numpy dtypes <https://numpy.org/doc/stable/reference/arrays.dtypes.html>`_ if possible.
            return_type (:obj:`type`): The return type of the function

        See also:
            :func:`register_aggregation`

        """
        self.functions[name.lower()] = FunctionDescription(
            f, parameters, return_type, False
        )

    def register_aggregation(
        self,
        f: dd.Aggregation,
        name: str,
        parameters: List[Tuple[str, type]],
        return_type: type,
    ):
        """
        Register a custom aggregation with the given name.
        The aggregation can be used (with this name)
        in every SQL queries from now on - but only for aggregation operations
        (no scalar function calls).
        This means, if you register a aggregation "fagg", you can now call

        .. code-block:: sql

            SELECT fagg(y)
            FROM df
            GROUP BY x

        Please note that you can always only have one function with the same name;
        no matter if it is an aggregation or scalar function.

        For the registration, you need to supply both the
        list of parameter and parameter types as well as the
        return type. Use `numpy dtypes <https://numpy.org/doc/stable/reference/arrays.dtypes.html>`_  if possible.

        More information: :ref:`custom`

        Example:
            The following code registers a new aggregation "fagg", which
            computes the sum of a column and uses it on the ``y`` column.

            .. code-block:: python

                fagg = dd.Aggregation("fagg", lambda x: x.sum(), lambda x: x.sum())
                c.register_aggregation(fagg, "fagg", [("x", np.float64)], np.float64)

                sql = "SELECT fagg(y) FROM df GROUP BY x"
                df_result = c.sql(sql)

        Args:
            f (:class:`dask.dataframe.Aggregate`): The aggregate to register. See
                `the dask documentation <https://docs.dask.org/en/latest/dataframe-groupby.html#aggregate>`_
                for more information.
            name (:obj:`str`): Under which name should the new aggregate be addressable in SQL
            parameters (:obj:`List[Tuple[str, type]]`): A list ot tuples of parameter name and parameter type.
                Use `numpy dtypes <https://numpy.org/doc/stable/reference/arrays.dtypes.html>`_ if possible.
            return_type (:obj:`type`): The return type of the function

        See also:
            :func:`register_function`

        """
        self.functions[name.lower()] = FunctionDescription(
            f, parameters, return_type, True
        )

    def sql(self, sql: str) -> dd.DataFrame:
        """
        Query the registered tables with the given SQL.
        The SQL follows approximately the postgreSQL standard - however, not all
        operations are already implemented.
        In general, only select statements (no data manipulation) works.

        For more information, see :ref:`sql`.

        Example:
            In this example, a query is called
            using the registered tables and then
            executed using dask.

            .. code-block:: python

                result = c.sql("SELECT a, b FROM my_table")
                print(result.compute())

        Args:
            sql (:obj:`str`): The query string to execute
            debug (:obj:`bool`): Turn on printing of debug information.

        Returns:
            :obj:`dask.dataframe.DataFrame`: the created data frame of this query.

        """
        try:
            rel, select_names = self._get_ral(sql)
            dc = RelConverter.convert(rel, context=self)
        except (ValidationException, SqlParseException) as e:
            logger.debug(f"Original exception raised by Java:\n {e}")
            # We do not want to re-raise an exception here
            # as this would print the full java stack trace
            # if debug is not set.
            # Instead, we raise a nice exception
            raise ParsingException(sql, str(e.message())) from None

        if dc is not None:
            if select_names:
                # Rename any columns named EXPR$* to a more human readable name
                cc = dc.column_container
                cc = cc.rename(
                    {
                        df_col: df_col
                        if not df_col.startswith("EXPR$")
                        else select_name
                        for df_col, select_name in zip(cc.columns, select_names)
                    }
                )
                dc = DataContainer(dc.df, cc)

            return dc.assign()

    def _prepare_schema(self):
        """
        Create a schema filled with the dataframes
        and functions we have currently in our list
        """
        schema = DaskSchema(self.schema_name)

        if not self.tables:  # pragma: no cover
            logger.warning("No tables are registered.")

        for name, dc in self.tables.items():
            table = DaskTable(name)
            df = dc.df
            logger.debug(
                f"Adding table '{name}' to schema with columns: {list(df.columns)}"
            )
            for column in df.columns:
                data_type = df[column].dtype
                sql_data_type = python_to_sql_type(data_type)

                table.addColumn(column, sql_data_type)

            schema.addTable(table)

        if not self.functions:
            logger.debug("No custom functions defined.")

        for name, function_description in self.functions.items():
            sql_return_type = python_to_sql_type(function_description.return_type)
            if function_description.aggregation:
                logger.debug(f"Adding function '{name}' to schema as aggregation.")
                dask_function = DaskAggregateFunction(name, sql_return_type)
            else:
                logger.debug(f"Adding function '{name}' to schema as scalar function.")
                dask_function = DaskScalarFunction(name, sql_return_type)
            self._add_parameters_from_description(function_description, dask_function)

            schema.addFunction(dask_function)

        return schema

    @staticmethod
    def _add_parameters_from_description(function_description, dask_function):
        for parameter in function_description.parameters:
            param_name, param_type = parameter
            sql_param_type = python_to_sql_type(param_type)

            dask_function.addParameter(param_name, sql_param_type, False)

    def _get_ral(self, sql):
        """Helper function to turn the sql query into a relational algebra and resulting column names"""
        # get the schema of what we currently have registered
        schema = self._prepare_schema()

        # Now create a relational algebra from that
        generator = RelationalAlgebraGenerator(schema)

        sqlNode = generator.getSqlNode(sql)
        sqlNodeClass = get_java_class(sqlNode)

        if sqlNodeClass.startswith("com.dask.sql.parser."):
            return sqlNode, []

        validatedSqlNode = generator.getValidatedNode(sqlNode)
        nonOptimizedRelNode = generator.getRelationalAlgebra(validatedSqlNode)
        rel = generator.getOptimizedRelationalAlgebra(nonOptimizedRelNode)
        default_dialect = generator.getDialect()

        logger.debug(f"Using dialect: {get_java_class(default_dialect)}")

        # Internal, temporary results of calcite are sometimes
        # named EXPR$N (with N a number), which is not very helpful
        # to the user. We replace these cases therefore with
        # the actual query string. This logic probably fails in some
        # edge cases (if the outer SQLNode is not a select node),
        # but so far I did not find such a case.
        # So please raise an issue if you have found one!
        def toSqlString(s):
            try:
                return str(s.toSqlString(default_dialect))
            except:  # pragma: no cover
                return str(s)

        if sqlNodeClass == "org.apache.calcite.sql.SqlSelect":
            select_names = [toSqlString(s) for s in sqlNode.getSelectList()]
        else:
            logger.debug(
                "Not extracting output column names as the SQL is not a SELECT call"
            )
            select_names = None

        logger.debug(
            f"Extracted relational algebra:\n {generator.getRelationalAlgebraString(rel)}"
        )

        return rel, select_names
