from typing import Callable, List, Tuple
from collections import namedtuple

import dask.dataframe as dd

from dask_sql.java import (
    DaskAggregateFunction,
    DaskScalarFunction,
    DaskSchema,
    DaskTable,
    RelationalAlgebraGenerator,
    SqlParseException,
    ValidationException,
)
from dask_sql.mappings import python_to_sql_type
from dask_sql.physical.rel import RelConverter, logical
from dask_sql.physical.rex import RexConverter, core
from dask_sql.utils import ParsingException

FunctionDescription = namedtuple(
    "FunctionDescription", ["f", "parameters", "return_type", "aggregation"]
)


class Context:
    """
    Main object to communicate with dask_sql.
    It holds a store of all registered tables and can convert
    SQL queries to dask dataframes.
    The tables in these queries are referenced by the name,
    which is given when registering a dask dataframe.


        from dask_sql import Context
        c = Context()

        # Register a table
        c.register_dask_table(df, "my_table")

        # Now execute an SQL query. The result is a dask dataframe
        result = c.sql("SELECT a, b FROM my_table")

        # Trigger the computation (or use the dataframe for something else)
        result.compute()

    """

    def __init__(self):
        """
        Create a new context.
        Usually, you will only ever have a single context
        in your program.
        """
        # Storage for the registered tables
        self.tables = {}
        # Storage for the registered functions
        self.functions = {}
        # Storage for the registered aggregations
        self.aggregations = {}

        # Register any default plugins, if nothing was registered before.
        RelConverter.add_plugin_class(logical.LogicalAggregatePlugin, replace=False)
        RelConverter.add_plugin_class(logical.LogicalFilterPlugin, replace=False)
        RelConverter.add_plugin_class(logical.LogicalJoinPlugin, replace=False)
        RelConverter.add_plugin_class(logical.LogicalProjectPlugin, replace=False)
        RelConverter.add_plugin_class(logical.LogicalSortPlugin, replace=False)
        RelConverter.add_plugin_class(logical.LogicalTableScanPlugin, replace=False)
        RelConverter.add_plugin_class(logical.LogicalUnionPlugin, replace=False)
        RelConverter.add_plugin_class(logical.LogicalValuesPlugin, replace=False)

        RexConverter.add_plugin_class(core.RexCallPlugin, replace=False)
        RexConverter.add_plugin_class(core.RexInputRefPlugin, replace=False)
        RexConverter.add_plugin_class(core.RexLiteralPlugin, replace=False)

    def register_dask_table(self, df: dd.DataFrame, name: str):
        """
        Registering a dask table makes it usable in SQl queries.
        The name you give here can be used as table name in the SQL later.

        Please note, that the table is stored as it is now.
        If you change the table later, you need to re-register.
        """
        self.tables[name.lower()] = df.copy()

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

            SELECT f(x)
            FROM df

        Please note that you can always only have one function with the same name;
        no matter if it is an aggregation or scalar function.

        For the registration, you need to supply both the
        list of parameter and parameter types as well as the
        return type. Use numpy dtypes if possible.
        Example:

            def f(x):
                return x ** 2

            c.register_function(f, "f", [("x", np.int64)], np.int64)

            sql = "SELECT f(x) FROM df"
            df_result = c.sql(sql)

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

            SELECT fagg(y)
            FROM df
            GROUP BY x

        Please note that you can always only have one function with the same name;
        no matter if it is an aggregation or scalar function.

        For the registration, you need to supply both the
        list of parameter and parameter types as well as the
        return type. Use numpy dtypes if possible.
        Example:

            fagg = dd.Aggregation("fagg", lambda x: x.sum(), lambda x: x.sum())
            c.register_aggregation(fagg, "fagg", [("x", np.float64)], np.float64)

            sql = "SELECT fagg(y) FROM df GROUP BY x"
            df_result = c.sql(sql)

        """
        self.functions[name.lower()] = FunctionDescription(
            f, parameters, return_type, True
        )

    def sql(self, sql: str, debug: bool = False) -> dd.DataFrame:
        """
        Query the registered tables with the given SQL.
        The SQL follows approximately the MYSQL standard - however, not all
        operations are already implemented.
        In general, only select statements (no data manipulation) works.
        """
        try:
            rel = self._get_ral(sql, debug=debug)
            df = RelConverter.convert(rel, context=self)
        except (ValidationException, SqlParseException) as e:
            if debug:
                from_chained_exception = e
            else:
                # We do not want to re-raise an exception here
                # as this would print the full java stack trace
                # if debug is not set.
                # Instead, we raise a nice exception
                from_chained_exception = None

            raise ParsingException(sql, str(e.message())) from from_chained_exception
        return df

    def _prepare_schema(self):
        """
        Create a schema filled with the dataframes
        and functions we have currently in our list
        """
        schema = DaskSchema("schema")

        for name, df in self.tables.items():
            table = DaskTable(name)
            for column in df.columns:
                data_type = df[column].dtype
                sql_data_type = python_to_sql_type(data_type)

                table.addColumn(column, sql_data_type)

            schema.addTable(table)

        for name, function_description in self.functions.items():
            sql_return_type = python_to_sql_type(function_description.return_type)
            if function_description.aggregation:
                dask_function = DaskAggregateFunction(name, sql_return_type)
            else:
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

    def _get_ral(self, sql, debug: bool = False):
        """Helper function to turn the sql query into a relational algebra"""
        # get the schema of what we currently have registered
        schema = self._prepare_schema()

        # Now create a relational algebra from that
        generator = RelationalAlgebraGenerator(schema)

        rel = generator.getRelationalAlgebra(sql)
        if debug:  # pragma: no cover
            print(generator.getRelationalAlgebraString(rel))

        return rel
