import asyncio
import inspect
import logging
import warnings
from collections import namedtuple
from typing import Any, Callable, Dict, List, Tuple, Union

import dask.dataframe as dd
import pandas as pd
from dask.base import optimize
from dask.distributed import Client

from dask_sql import input_utils
from dask_sql.datacontainer import DataContainer, FunctionDescription, SchemaContainer
from dask_sql.input_utils import InputType, InputUtil
from dask_sql.integrations.ipython import ipython_integration
from dask_sql.java import (
    DaskAggregateFunction,
    DaskScalarFunction,
    DaskSchema,
    DaskTable,
    RelationalAlgebraGenerator,
    RelationalAlgebraGeneratorBuilder,
    SqlParseException,
    ValidationException,
    get_java_class,
)
from dask_sql.mappings import python_to_sql_type
from dask_sql.physical.rel import RelConverter, custom, logical
from dask_sql.physical.rex import RexConverter, core
from dask_sql.utils import ParsingException

logger = logging.getLogger(__name__)


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

    DEFAULT_SCHEMA_NAME = "root"

    def __init__(self):
        """
        Create a new context.
        """
        # Name of the root schema
        self.schema_name = self.DEFAULT_SCHEMA_NAME
        # All schema information
        self.schema = {self.schema_name: SchemaContainer(self.schema_name)}
        # A started SQL server (useful for jupyter notebooks)
        self.sql_server = None

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
        RelConverter.add_plugin_class(custom.CreateExperimentPlugin, replace=False)
        RelConverter.add_plugin_class(custom.CreateModelPlugin, replace=False)
        RelConverter.add_plugin_class(custom.CreateSchemaPlugin, replace=False)
        RelConverter.add_plugin_class(custom.CreateTableAsPlugin, replace=False)
        RelConverter.add_plugin_class(custom.CreateTablePlugin, replace=False)
        RelConverter.add_plugin_class(custom.DropModelPlugin, replace=False)
        RelConverter.add_plugin_class(custom.DropSchemaPlugin, replace=False)
        RelConverter.add_plugin_class(custom.DropTablePlugin, replace=False)
        RelConverter.add_plugin_class(custom.ExportModelPlugin, replace=False)
        RelConverter.add_plugin_class(custom.PredictModelPlugin, replace=False)
        RelConverter.add_plugin_class(custom.ShowColumnsPlugin, replace=False)
        RelConverter.add_plugin_class(custom.ShowModelParamsPlugin, replace=False)
        RelConverter.add_plugin_class(custom.ShowModelsPlugin, replace=False)
        RelConverter.add_plugin_class(custom.ShowSchemasPlugin, replace=False)
        RelConverter.add_plugin_class(custom.ShowTablesPlugin, replace=False)
        RelConverter.add_plugin_class(custom.SwitchSchemaPlugin, replace=False)

        RexConverter.add_plugin_class(core.RexCallPlugin, replace=False)
        RexConverter.add_plugin_class(core.RexInputRefPlugin, replace=False)
        RexConverter.add_plugin_class(core.RexLiteralPlugin, replace=False)
        RexConverter.add_plugin_class(core.RexOverPlugin, replace=False)

        InputUtil.add_plugin_class(input_utils.DaskInputPlugin, replace=False)
        InputUtil.add_plugin_class(input_utils.PandasInputPlugin, replace=False)
        InputUtil.add_plugin_class(input_utils.HiveInputPlugin, replace=False)
        InputUtil.add_plugin_class(input_utils.IntakeCatalogInputPlugin, replace=False)
        InputUtil.add_plugin_class(input_utils.SqlalchemyHiveInputPlugin, replace=False)
        # needs to be the last entry, as it only checks for string
        InputUtil.add_plugin_class(input_utils.LocationInputPlugin, replace=False)

    def create_table(
        self,
        table_name: str,
        input_table: InputType,
        format: str = None,
        persist: bool = True,
        schema_name: str = None,
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

        schema_name = schema_name or self.schema_name

        dc = InputUtil.to_dc(
            input_table,
            table_name=table_name,
            format=format,
            persist=persist,
            **kwargs,
        )
        self.schema[schema_name].tables[table_name.lower()] = dc

    def register_dask_table(self, df: dd.DataFrame, name: str, *args, **kwargs):
        """
        Outdated version of :func:`create_table()`.
        """
        warnings.warn(
            "register_dask_table is deprecated, use the more general create_table instead.",
            DeprecationWarning,
        )
        return self.create_table(name, df, *args, **kwargs)

    def drop_table(self, table_name: str, schema_name: str = None):
        """
        Remove a table with the given name from the registered tables.
        This will also delete the dataframe.

        Args:
            table_name: (:obj:`str`): Which table to remove.

        """
        schema_name = schema_name or self.schema_name
        del self.schema[schema_name].tables[table_name]

    def drop_schema(self, schema_name: str):
        """
        Remove a schema with the given name from the registered schemas.
        This will also delete all tables, functions etc.

        Args:
            schema_name: (:obj:`str`): Which schema to remove.

        """
        if schema_name == self.DEFAULT_SCHEMA_NAME:
            raise RuntimeError(f"Default Schema `{schema_name}` cannot be deleted")

        del self.schema[schema_name]

        if self.schema_name == schema_name:
            self.schema_name = self.DEFAULT_SCHEMA_NAME

    def register_function(
        self,
        f: Callable,
        name: str,
        parameters: List[Tuple[str, type]],
        return_type: type,
        replace: bool = False,
        schema_name: str = None,
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
            replace (:obj:`bool`): Do not raise an error if the function is already present

        See also:
            :func:`register_aggregation`

        """
        self._register_callable(
            f,
            name,
            aggregation=False,
            parameters=parameters,
            return_type=return_type,
            replace=replace,
            schema_name=schema_name,
        )

    def register_aggregation(
        self,
        f: dd.Aggregation,
        name: str,
        parameters: List[Tuple[str, type]],
        return_type: type,
        replace: bool = False,
        schema_name: str = None,
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
            replace (:obj:`bool`): Do not raise an error if the function is already present

        See also:
            :func:`register_function`

        """
        self._register_callable(
            f,
            name,
            aggregation=True,
            parameters=parameters,
            return_type=return_type,
            replace=replace,
            schema_name=schema_name,
        )

    def sql(
        self,
        sql: str,
        return_futures: bool = True,
        dataframes: Dict[str, Union[dd.DataFrame, pd.DataFrame]] = None,
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
            dataframes (:obj:`Dict[str, dask.dataframe.DataFrame]`): additional Dask or pandas dataframes
                to register before executing this query

        Returns:
            :obj:`dask.dataframe.DataFrame`: the created data frame of this query.

        """
        if dataframes is not None:
            for df_name, df in dataframes.items():
                self.create_table(df_name, df)

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

    def explain(
        self, sql: str, dataframes: Dict[str, Union[dd.DataFrame, pd.DataFrame]] = None
    ) -> str:
        """
        Return the stringified relational algebra that this query will produce
        once triggered (with ``sql()``).
        Helpful to understand the inner workings of dask-sql, but typically not
        needed to query your data.

        If the query is of DDL type (e.g. CREATE TABLE or DESCRIBE SCHEMA),
        no relational algebra plan is created and therefore nothing returned.

        Args:
            sql (:obj:`str`): The query string to use
            dataframes (:obj:`Dict[str, dask.dataframe.DataFrame]`): additional Dask or pandas dataframes
                to register before executing this query

        Returns:
            :obj:`str`: a description of the created relational algebra.

        """
        if dataframes is not None:
            for df_name, df in dataframes.items():
                self.create_table(df_name, df)

        _, _, rel_string = self._get_ral(sql)
        return rel_string

    def visualize(self, sql: str, filename="mydask.png") -> None:  # pragma: no cover
        """Visualize the computation of the given SQL into the png"""
        result = self.sql(sql, return_futures=True)
        (result,) = optimize(result)

        result.visualize(filename)

    def create_schema(self, schema_name: str):
        """
        Create a new schema in the database.

        Args:
            schema_name (:obj:`str`): The name of the schema to create
        """
        self.schema[schema_name] = SchemaContainer(schema_name)

    def register_experiment(
        self,
        experiment_name: str,
        experiment_results: pd.DataFrame,
        schema_name: str = None,
    ):
        schema_name = schema_name or self.schema_name
        self.schema[schema_name].experiments[
            experiment_name.lower()
        ] = experiment_results

    def register_model(
        self,
        model_name: str,
        model: Any,
        training_columns: List[str],
        schema_name: str = None,
    ):
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
        schema_name = schema_name or self.schema_name
        self.schema[schema_name].models[model_name.lower()] = (model, training_columns)

    def ipython_magic(self, auto_include=False):  # pragma: no cover
        """
        Register a new ipython/jupyter magic function "sql"
        which sends its input as string to the :func:`sql` function.
        After calling this magic function in a Jupyter notebook or
        an IPython shell, you can write

        .. code-block:: python

            %sql SELECT * from data

        or

        .. code-block:: python

            %%sql
            SELECT * from data

        instead of

        .. code-block:: python

            c.sql("SELECT * from data")

        Args:
            auto_include (:obj:`bool`): If set to true, automatically
                create a table for every pandas or Dask dataframe in the calling
                context. That means, if you define a dataframe in your jupyter
                notebook you can use it with the same name in your sql call.
                Use this setting with care as any defined dataframe can
                easily override tables created via `CREATE TABLE`.

                .. code-block:: python

                    df = ...

                    # Later, without any calls to create_table

                    %%sql
                    SELECT * FROM df

        """
        ipython_integration(self, auto_include=auto_include)

    def run_server(
        self,
        client: Client = None,
        host: str = "0.0.0.0",
        port: int = 8080,
        log_level=None,
        blocking: bool = True,
    ):  # pragma: no cover
        """
        Run a HTTP server for answering SQL queries using ``dask-sql``.

        See :ref:`server` for more information.

        Args:
            client (:obj:`dask.distributed.Client`): If set, use this dask client instead of a new one.
            host (:obj:`str`): The host interface to listen on (defaults to all interfaces)
            port (:obj:`int`): The port to listen on (defaults to 8080)
            log_level: (:obj:`str`): The log level of the server and dask-sql
        """
        from dask_sql.server.app import run_server

        self.stop_server()
        self.server = run_server(
            context=self,
            client=client,
            host=host,
            port=port,
            log_level=log_level,
            blocking=blocking,
        )

    def stop_server(self):  # pragma: no cover
        """
        Stop a SQL server started by ``run_server`.
        """
        if not self.sql_server is None:
            loop = asyncio.get_event_loop()
            assert loop
            loop.create_task(self.sql_server.shutdown())

        self.sql_server = None

    def fqn(
        self, identifier: "org.apache.calcite.sql.SqlIdentifier"
    ) -> Tuple[str, str]:
        """
        Return the fully qualified name of an object, maybe including the schema name.

        Args:
            identifier (:obj:`str`): The Java identifier of the table or view

        Returns:
            :obj:`tuple` of :obj:`str`: The fully qualified name of the object
        """
        components = [str(n) for n in identifier.names]
        if len(components) == 2:
            schema = components[0]
            name = components[1]
        elif len(components) == 1:
            schema = self.schema_name
            name = components[0]
        else:
            raise AttributeError(
                f"Do not understand the identifier {identifier} (too many components)"
            )

        return schema, name

    def _prepare_schemas(self):
        """
        Create a list of schemas filled with the dataframes
        and functions we have currently in our schema list
        """
        schema_list = []

        for schema_name, schema in self.schema.items():
            java_schema = DaskSchema(schema_name)

            if not schema.tables:
                logger.warning("No tables are registered.")

            for name, dc in schema.tables.items():
                table = DaskTable(name)
                df = dc.df
                logger.debug(
                    f"Adding table '{name}' to schema with columns: {list(df.columns)}"
                )
                for column in df.columns:
                    data_type = df[column].dtype
                    sql_data_type = python_to_sql_type(data_type)

                    table.addColumn(column, sql_data_type)

                java_schema.addTable(table)

            if not schema.functions:
                logger.debug("No custom functions defined.")

            for function_description in schema.function_lists:
                name = function_description.name
                sql_return_type = python_to_sql_type(function_description.return_type)
                if function_description.aggregation:
                    logger.debug(f"Adding function '{name}' to schema as aggregation.")
                    dask_function = DaskAggregateFunction(name, sql_return_type)
                else:
                    logger.debug(
                        f"Adding function '{name}' to schema as scalar function."
                    )
                    dask_function = DaskScalarFunction(name, sql_return_type)

                dask_function = self._add_parameters_from_description(
                    function_description, dask_function
                )

                java_schema.addFunction(dask_function)

            schema_list.append(java_schema)

        return schema_list

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
        schemas = self._prepare_schemas()

        # Now create a relational algebra from that
        generator_builder = RelationalAlgebraGeneratorBuilder(self.schema_name)
        for schema in schemas:
            generator_builder.addSchema(schema)
        generator = generator_builder.build()
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
            default_dialect = RelationalAlgebraGenerator.getDialect()

        try:
            return str(s.toSqlString(default_dialect))
        except:  # pragma: no cover. Have not seen any instance so far, but better be safe than sorry.
            return str(s)

    def _get_tables_from_stack(self):
        """Helper function to return all dask/pandas dataframes from the calling stack"""
        stack = inspect.stack()

        tables = {}

        # Traverse the stacks from inside to outside
        for frame_info in stack:
            for var_name, variable in frame_info.frame.f_locals.items():
                if var_name.startswith("_"):
                    continue
                if not isinstance(variable, (pd.DataFrame, dd.DataFrame)):
                    continue

                # only set them if not defined in an inner context
                tables[var_name] = tables.get(var_name, variable)

        return tables

    def _register_callable(
        self,
        f: Any,
        name: str,
        aggregation: bool,
        parameters: List[Tuple[str, type]],
        return_type: type,
        replace: bool = False,
        schema_name=None,
    ):
        """Helper function to do the function or aggregation registration"""
        schema_name = schema_name or self.schema_name
        schema = self.schema[schema_name]

        lower_name = name.lower()
        if lower_name in schema.functions:
            if replace:
                schema.function_lists = list(
                    filter(
                        lambda f: f.name.lower() != lower_name, schema.function_lists,
                    )
                )
                del schema.functions[lower_name]

            elif schema.functions[lower_name] != f:
                raise ValueError(
                    "Registering different functions with the same name is not allowed"
                )

        schema.function_lists.append(
            FunctionDescription(name.upper(), parameters, return_type, aggregation)
        )
        schema.function_lists.append(
            FunctionDescription(name.lower(), parameters, return_type, aggregation)
        )
        schema.functions[lower_name] = f
