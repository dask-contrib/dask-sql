from typing import Any, Callable, Dict, List, Tuple, Union
from collections import namedtuple
import logging
import warnings

import dask.dataframe as dd
import pandas as pd

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
from dask_sql import input_utils
from dask_sql.input_utils import InputType, InputUtil
from dask_sql.mappings import python_to_sql_type
from dask_sql.physical.rel import RelConverter, logical, custom
from dask_sql.physical.rex import RexConverter, core
from dask_sql.datacontainer import DataContainer
from dask_sql.utils import ParsingException

logger = logging.getLogger(__name__)

FunctionDescription = namedtuple(
    "FunctionDescription", ["name", "parameters", "return_type", "aggregation"]
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
            c.create_table("my_table", df)

            # Now execute an SQL query. The result is a dask dataframe
            result = c.sql("SELECT a, b FROM my_table")

            # Trigger the computation (or use the data frame for something else)
            result.compute()

    Usually, you will only ever have a single context in your program.

    See also:
        :func:`sql`
        :func:`create_table`

    """

    def __init__(self):
        """
        Create a new context.
        """
        # Storage for the registered tables
        self.tables = {}
        # Storage for the registered functions
        self.functions: Dict[str, Callable] = {}
        self.function_list: List[FunctionDescription] = []
        # Storage for the registered aggregations
        self.aggregations = {}
        # Storage for the trained models
        self.models = {}
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
        RelConverter.add_plugin_class(logical.SamplePlugin, replace=False)
        RelConverter.add_plugin_class(custom.AnalyzeTablePlugin, replace=False)
        RelConverter.add_plugin_class(custom.CreateModelPlugin, replace=False)
        RelConverter.add_plugin_class(custom.CreateTableAsPlugin, replace=False)
        RelConverter.add_plugin_class(custom.CreateTablePlugin, replace=False)
        RelConverter.add_plugin_class(custom.PredictModelPlugin, replace=False)
        RelConverter.add_plugin_class(custom.DropModelPlugin, replace=False)
        RelConverter.add_plugin_class(custom.DropTablePlugin, replace=False)
        RelConverter.add_plugin_class(custom.ShowColumnsPlugin, replace=False)
        RelConverter.add_plugin_class(custom.ShowSchemasPlugin, replace=False)
        RelConverter.add_plugin_class(custom.ShowTablesPlugin, replace=False)

        RexConverter.add_plugin_class(core.RexCallPlugin, replace=False)
        RexConverter.add_plugin_class(core.RexInputRefPlugin, replace=False)
        RexConverter.add_plugin_class(core.RexLiteralPlugin, replace=False)

        InputUtil.add_plugin_class(input_utils.DaskInputPlugin, replace=False)
        InputUtil.add_plugin_class(input_utils.PandasInputPlugin, replace=False)
        InputUtil.add_plugin_class(input_utils.HiveInputPlugin, replace=False)
        InputUtil.add_plugin_class(input_utils.IntakeCatalogInputPlugin, replace=False)
        # needs to be the last entry, as it only checks for string
        InputUtil.add_plugin_class(input_utils.LocationInputPlugin, replace=False)

    def create_table(
        self,
        table_name: str,
        input_table: InputType,
        format: str = None,
        persist: bool = True,
        **kwargs,
    ):
        """
        Registering a (dask/pandas) table makes it usable in SQL queries.
        The name you give here can be used as table name in the SQL later.

        Please note, that the table is stored as it is now.
        If you change the table later, you need to re-register.

        Instead of passing an already loaded table, it is also possible
        to pass a string to a storage location.
        The library will then try to load the data using one of
        `dask's read methods <https://docs.dask.org/en/latest/dataframe-create.html>`_.
        If the file format can not be deduced automatically, it is also
        possible to specify it via the ``format`` parameter.
        Typical file formats are csv or parquet.
        Any additional parameters will get passed on to the read method.
        Please note that some file formats require additional libraries.
        By default, the data will be loaded directly into the memory
        of the nodes. If you do not want that, set persist to False.

        See :ref:`data_input` for more information.

        Example:
            This code registers a data frame as table "data"
            and then uses it in a query.

            .. code-block:: python

                c.create_table("data", df)
                df_result = c.sql("SELECT a, b FROM data")

            This code reads a file from disk.
            Please note that we assume that the file(s) are reachable under this path
            from every node in the cluster

            .. code-block:: python

                c.create_table("data", "/home/user/data.csv")
                df_result = c.sql("SELECT a, b FROM data")

            This example reads from a hive table.

            .. code-block:: python

                from pyhive.hive import connect

                cursor = connect("localhost", 10000).cursor()
                c.create_table("data", cursor, hive_table_name="the_name_in_hive")
                df_result = c.sql("SELECT a, b FROM data")

        Args:
            table_name: (:obj:`str`): Under which name should the new table be addressable
            input_table (:class:`dask.dataframe.DataFrame` or :class:`pandas.DataFrame` or :obj:`str` or :class:`hive.Cursor`):
                The data frame/location/hive connection to register.
            format (:obj:`str`): Only used when passing a string into the ``input`` parameter.
                Specify the file format directly here if it can not be deduced from the extension.
                If set to "memory", load the data from a published dataset in the dask cluster.
            persist (:obj:`bool`): Only used when passing a string into the ``input`` parameter.
                Set to false to turn off loading the file data directly into memory.
            **kwargs: Additional arguments for specific formats. See :ref:`data_input` for more information.

        """
        if "file_format" in kwargs:  # pragma: no cover
            warnings.warn("file_format is renamed to format", DeprecationWarning)
            format = kwargs.pop("file_format")

        dc = InputUtil.to_dc(
            input_table,
            table_name=table_name,
            format=format,
            persist=persist,
            **kwargs,
        )
        self.tables[table_name.lower()] = dc

    def register_dask_table(self, df: dd.DataFrame, name: str):
        """
        Outdated version of :func:`create_table()`.
        """
        warnings.warn(
            "register_dask_table is deprecated, use the more general create_table instead.",
            DeprecationWarning,
        )
        return self.create_table(name, df)

    def drop_table(self, table_name: str):
        """
        Remove a table with the given name from the registered tables.
        This will also delete the dataframe.

        Args:
            table_name: (:obj:`str`): Which table to remove.

        """
        del self.tables[table_name]

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
        self.function_list.append(
            FunctionDescription(name, parameters, return_type, False)
        )

        name = name.lower()
        if name in self.functions:
            if self.functions[name] != f:
                raise ValueError(
                    "Registering different functions with the same name is not allowed"
                )
        self.functions[name] = f

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
        self.function_list.append(
            FunctionDescription(name, parameters, return_type, True)
        )

        name = name.lower()
        if name in self.functions:
            if self.functions[name] != f:
                raise ValueError(
                    "Registering different functions with the same name is not allowed"
                )
        self.functions[name] = f

    def sql(
        self, sql: str, return_futures: bool = True
    ) -> Union[dd.DataFrame, pd.DataFrame]:
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
            return_futures (:obj:`bool`): Return the unexecuted dask dataframe or the data itself.
                Defaults to returning the dask dataframe.

        Returns:
            :obj:`dask.dataframe.DataFrame`: the created data frame of this query.

        """
        rel, select_names, _ = self._get_ral(sql)

        dc = RelConverter.convert(rel, context=self)

        if dc is None:
            return

        if select_names:
            # Rename any columns named EXPR$* to a more human readable name
            cc = dc.column_container
            cc = cc.rename(
                {
                    df_col: df_col if not df_col.startswith("EXPR$") else select_name
                    for df_col, select_name in zip(cc.columns, select_names)
                }
            )
            dc = DataContainer(dc.df, cc)

        df = dc.assign()
        if not return_futures:
            df = df.compute()

        return df

    def explain(self, sql: str) -> str:
        """
        Return the stringified relational algebra that this query will produce
        once triggered (with ``sql()``).
        Helpful to understand the inner workings of dask-sql, but typically not
        needed to query your data.

        If the query is of DDL type (e.g. CREATE TABLE or DESCRIBE SCHEMA),
        no relational algebra plan is created and therefore nothing returned.

        Args:
            sql (:obj:`str`): The query string to use

        Returns:
            :obj:`str`: a description of the created relational algebra.

        """
        _, _, rel_string = self._get_ral(sql)
        return rel_string

    def register_model(self, model_name: str, model: Any, training_columns: List[str]):
        """
        Add a model to the model registry.
        A model can be anything which has a `.predict` function that transforms
        a Dask dataframe into predicted labels (as a Dask series).
        After model registration, the model can be used in calls to
        `SELECT ... FROM PREDICT` with the given name.
        Instead of creating your own model and register it, you can also
        train a model directly in dask-sql. See the SQL command `CrEATE MODEL`.

        Args:
            model_name (:obj:`str`): The name of the model
            model: The model to store
            training_columns: (list of str): The names of the columns which were
                used during the training.
        """
        self.models[model_name] = (model, training_columns)

    def _prepare_schema(self):
        """
        Create a schema filled with the dataframes
        and functions we have currently in our list
        """
        schema = DaskSchema(self.schema_name)

        if not self.tables:
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

        for function_description in self.function_list:
            name = function_description.name
            sql_return_type = python_to_sql_type(function_description.return_type)
            if function_description.aggregation:
                logger.debug(f"Adding function '{name}' to schema as aggregation.")
                dask_function = DaskAggregateFunction(name, sql_return_type)
            else:
                logger.debug(f"Adding function '{name}' to schema as scalar function.")
                dask_function = DaskScalarFunction(name, sql_return_type)

            dask_function = self._add_parameters_from_description(
                function_description, dask_function
            )

            schema.addFunction(dask_function)

        return schema

    @staticmethod
    def _add_parameters_from_description(function_description, dask_function):
        for parameter in function_description.parameters:
            param_name, param_type = parameter
            sql_param_type = python_to_sql_type(param_type)

            dask_function.addParameter(param_name, sql_param_type, False)

        return dask_function

    def _get_ral(self, sql):
        """Helper function to turn the sql query into a relational algebra and resulting column names"""
        # get the schema of what we currently have registered
        schema = self._prepare_schema()

        # Now create a relational algebra from that
        generator = RelationalAlgebraGenerator(schema)
        default_dialect = generator.getDialect()

        logger.debug(f"Using dialect: {get_java_class(default_dialect)}")

        try:
            sqlNode = generator.getSqlNode(sql)
            sqlNodeClass = get_java_class(sqlNode)

            if sqlNodeClass.startswith("com.dask.sql.parser."):
                rel = sqlNode
                rel_string = ""
            else:
                validatedSqlNode = generator.getValidatedNode(sqlNode)
                nonOptimizedRelNode = generator.getRelationalAlgebra(validatedSqlNode)
                rel = generator.getOptimizedRelationalAlgebra(nonOptimizedRelNode)
                rel_string = str(generator.getRelationalAlgebraString(rel))
        except (ValidationException, SqlParseException) as e:
            logger.debug(f"Original exception raised by Java:\n {e}")
            # We do not want to re-raise an exception here
            # as this would print the full java stack trace
            # if debug is not set.
            # Instead, we raise a nice exception
            raise ParsingException(sql, str(e.message())) from None

        # Internal, temporary results of calcite are sometimes
        # named EXPR$N (with N a number), which is not very helpful
        # to the user. We replace these cases therefore with
        # the actual query string. This logic probably fails in some
        # edge cases (if the outer SQLNode is not a select node),
        # but so far I did not find such a case.
        # So please raise an issue if you have found one!
        if sqlNodeClass == "org.apache.calcite.sql.SqlOrderBy":
            sqlNode = sqlNode.query
            sqlNodeClass = get_java_class(sqlNode)

        if sqlNodeClass == "org.apache.calcite.sql.SqlSelect":
            select_names = [
                self._to_sql_string(s, default_dialect=default_dialect)
                for s in sqlNode.getSelectList()
            ]
        else:
            logger.debug(
                "Not extracting output column names as the SQL is not a SELECT call"
            )
            select_names = None

        logger.debug(f"Extracted relational algebra:\n {rel_string}")
        return rel, select_names, rel_string

    def _to_sql_string(self, s: "org.apache.calcite.sql.SqlNode", default_dialect=None):
        if default_dialect is None:
            schema = self._prepare_schema()

            generator = RelationalAlgebraGenerator(schema)
            default_dialect = generator.getDialect()

        try:
            return str(s.toSqlString(default_dialect))
        except:  # pragma: no cover. Have not seen any instance so far, but better be safe than sorry.
            return str(s)
