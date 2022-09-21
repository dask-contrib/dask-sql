import asyncio
import inspect
import logging
import warnings
from collections import Counter
from typing import Any, Callable, Dict, List, Tuple, Union

import dask.dataframe as dd
import pandas as pd
from dask import config as dask_config
from dask.base import optimize
from dask.distributed import Client

from dask_planner.rust import (
    DaskSchema,
    DaskSQLContext,
    DaskTable,
    DFOptimizationException,
    DFParsingException,
    LogicalPlan,
)

try:
    import dask_cuda  # noqa: F401
except ImportError:  # pragma: no cover
    pass

from dask_sql import input_utils
from dask_sql.datacontainer import (
    UDF,
    DataContainer,
    FunctionDescription,
    SchemaContainer,
    Statistics,
)
from dask_sql.input_utils import InputType, InputUtil
from dask_sql.integrations.ipython import ipython_integration
from dask_sql.mappings import python_to_sql_type
from dask_sql.physical.rel import RelConverter, custom, logical
from dask_sql.physical.rex import RexConverter, core
from dask_sql.utils import OptimizationException, ParsingException

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

    DEFAULT_CATALOG_NAME = "dask_sql"
    DEFAULT_SCHEMA_NAME = "root"

    def __init__(self, logging_level=logging.INFO):
        """
        Create a new context.
        """

        # Set the logging level for this SQL context
        logging.basicConfig(level=logging_level)

        # Name of the root catalog
        self.catalog_name = self.DEFAULT_CATALOG_NAME
        # Name of the root schema
        self.schema_name = self.DEFAULT_SCHEMA_NAME
        # All schema information
        self.schema = {self.schema_name: SchemaContainer(self.schema_name)}
        # A started SQL server (useful for jupyter notebooks)
        self.sql_server = None

        # Create the `DaskSQLContext` Rust context
        self.context = DaskSQLContext(self.catalog_name, self.schema_name)
        self.context.register_schema(self.schema_name, DaskSchema(self.schema_name))

        # # Register any default plugins, if nothing was registered before.
        RelConverter.add_plugin_class(logical.DaskAggregatePlugin, replace=False)
        RelConverter.add_plugin_class(logical.DaskCrossJoinPlugin, replace=False)
        RelConverter.add_plugin_class(logical.DaskEmptyRelationPlugin, replace=False)
        RelConverter.add_plugin_class(logical.DaskFilterPlugin, replace=False)
        RelConverter.add_plugin_class(logical.DaskJoinPlugin, replace=False)
        RelConverter.add_plugin_class(logical.DaskLimitPlugin, replace=False)
        RelConverter.add_plugin_class(logical.DaskProjectPlugin, replace=False)
        RelConverter.add_plugin_class(logical.DaskSortPlugin, replace=False)
        RelConverter.add_plugin_class(logical.DaskTableScanPlugin, replace=False)
        RelConverter.add_plugin_class(logical.DaskUnionPlugin, replace=False)
        RelConverter.add_plugin_class(logical.DaskValuesPlugin, replace=False)
        RelConverter.add_plugin_class(logical.DaskWindowPlugin, replace=False)
        RelConverter.add_plugin_class(logical.SamplePlugin, replace=False)
        RelConverter.add_plugin_class(logical.ExplainPlugin, replace=False)
        RelConverter.add_plugin_class(logical.SubqueryAlias, replace=False)
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
        RelConverter.add_plugin_class(custom.AlterSchemaPlugin, replace=False)
        RelConverter.add_plugin_class(custom.AlterTablePlugin, replace=False)
        RelConverter.add_plugin_class(custom.DistributeByPlugin, replace=False)

        RexConverter.add_plugin_class(core.RexCallPlugin, replace=False)
        RexConverter.add_plugin_class(core.RexInputRefPlugin, replace=False)
        RexConverter.add_plugin_class(core.RexLiteralPlugin, replace=False)
        RexConverter.add_plugin_class(core.RexSubqueryAliasPlugin, replace=False)

        InputUtil.add_plugin_class(input_utils.DaskInputPlugin, replace=False)
        InputUtil.add_plugin_class(input_utils.PandasLikeInputPlugin, replace=False)
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
        persist: bool = False,
        schema_name: str = None,
        statistics: Statistics = None,
        gpu: bool = False,
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
        By default, the data will be lazily loaded. If you would like to
        load the data directly into memory you can do so by setting
        persist=True.

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
                Set to true to turn on loading the file data directly into memory.
            schema_name: (:obj:`str`): in which schema to create the table. By default, will use the currently selected schema.
            statistics: (:obj:`Statistics`): if given, use these statistics during the cost-based optimization. If no
                statistics are provided, we will just assume 100 rows.
            gpu: (:obj:`bool`): if set to true, use dask-cudf to run the data frame calculations on your GPU.
                Please note that the GPU support is currently not covering all of dask-sql's SQL language.
            **kwargs: Additional arguments for specific formats. See :ref:`data_input` for more information.

        """
        logger.debug(
            f"Creating table: '{table_name}' of format type '{format}' in schema '{schema_name}'"
        )
        if "file_format" in kwargs:  # pragma: no cover
            warnings.warn("file_format is renamed to format", DeprecationWarning)
            format = kwargs.pop("file_format")

        schema_name = schema_name or self.schema_name

        dc = InputUtil.to_dc(
            input_table,
            table_name=table_name,
            format=format,
            persist=persist,
            gpu=gpu,
            **kwargs,
        )

        self.schema[schema_name].tables[table_name.lower()] = dc
        if statistics:
            self.schema[schema_name].statistics[table_name.lower()] = statistics

        # Register the table with the Rust DaskSQLContext
        self.context.register_table(
            schema_name, DaskTable(schema_name, table_name, 100)
        )

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
        row_udf: bool = False,
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

        Please keep in mind that you can only have one function with the same name,
        regardless of whether it is an aggregation or a scalar function. By default,
        attempting to register two functions with the same name will raise an error;
        setting `replace=True` will give precedence to the most recently registered
        function.

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

        Example of overwriting two functions with the same name:
            This example registers a different function "f", which
            calculates the floor division of an integer and applies
            it to the column ``x``. It also shows how to overwrite
            the previous function with the replace parameter.

            .. code-block:: python

                def f(x):
                    return x // 2

                c.register_function(f, "f", [("x", np.int64)], np.int64, replace=True)

                sql = "SELECT f(x) FROM df"
                df_result = c.sql(sql)

        Args:
            f (:obj:`Callable`): The function to register
            name (:obj:`str`): Under which name should the new function be addressable in SQL
            parameters (:obj:`List[Tuple[str, type]]`): A list ot tuples of parameter name and parameter type.
                Use `numpy dtypes <https://numpy.org/doc/stable/reference/arrays.dtypes.html>`_ if possible. This
                function is sensitive to the order of specified parameters when `row_udf=True`, and it is assumed
                that column arguments are specified in order, followed by scalar arguments.
            return_type (:obj:`type`): The return type of the function
            replace (:obj:`bool`): If `True`, do not raise an error if a function with the same name is already
            present; instead, replace the original function. Default is `False`.

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
            row_udf=row_udf,
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
        sql: Any,
        return_futures: bool = True,
        dataframes: Dict[str, Union[dd.DataFrame, pd.DataFrame]] = None,
        gpu: bool = False,
        config_options: Dict[str, Any] = None,
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
            gpu (:obj:`bool`): Whether or not to load the additional Dask or pandas dataframes (if any) on GPU;
                requires cuDF / dask-cuDF if enabled. Defaults to False.
            config_options (:obj:`Dict[str,Any]`): Specific configuration options to pass during
                query execution
        Returns:
            :obj:`dask.dataframe.DataFrame`: the created data frame of this query.
        """
        with dask_config.set(config_options):
            if dataframes is not None:
                for df_name, df in dataframes.items():
                    self.create_table(df_name, df, gpu=gpu)

            if isinstance(sql, str):
                rel, _ = self._get_ral(sql)
            elif isinstance(sql, LogicalPlan):
                rel = sql
            else:
                raise RuntimeError(
                    f"Encountered unsupported `LogicalPlan` sql type: {type(sql)}"
                )

            return self._compute_table_from_rel(rel, return_futures)

    def explain(
        self,
        sql: str,
        dataframes: Dict[str, Union[dd.DataFrame, pd.DataFrame]] = None,
        gpu: bool = False,
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
            gpu (:obj:`bool`): Whether or not to load the additional Dask or pandas dataframes (if any) on GPU;
                requires cuDF / dask-cuDF if enabled. Defaults to False.

        Returns:
            :obj:`str`: a description of the created relational algebra.

        """
        if dataframes is not None:
            for df_name, df in dataframes.items():
                self.create_table(df_name, df, gpu=gpu)

        _, rel_string = self._get_ral(sql)
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

    def alter_schema(self, old_schema_name, new_schema_name):
        """
        Alter schema

        Args:
             old_schema_name:
             new_schema_name:
        """
        self.schema[new_schema_name] = self.schema.pop(old_schema_name)

    def alter_table(self, old_table_name, new_table_name):
        """
        Alter Table

        Args:
            old_table_name:
            new_table_name:
        """
        self.schema[self.schema_name].tables[new_table_name] = self.schema[
            self.schema_name
        ].tables.pop(old_table_name)

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

    def ipython_magic(
        self, auto_include=False, disable_highlighting=True
    ):  # pragma: no cover
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

            disable_highlighting (:obj:`bool`): If set to true, automatically
                disable syntax highlighting. If you are working in jupyter lab,
                diable_highlighting must be set to true to enable ipython_magic
                functionality. If you are working in a classic jupyter notebook,
                you may set disable_highlighting=False if desired.
        """
        ipython_integration(
            self, auto_include=auto_include, disable_highlighting=disable_highlighting
        )

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
        Stop a SQL server started by ``run_server``.
        """
        if self.sql_server is not None:
            loop = asyncio.get_event_loop()
            assert loop
            loop.create_task(self.sql_server.shutdown())

        self.sql_server = None

    def fqn(self, tbl: "DaskTable") -> Tuple[str, str]:
        """
        Return the fully qualified name of an object, maybe including the schema name.

        Args:
            tbl (:obj:`DaskTable`): The Rust DaskTable instance of the view or table.

        Returns:
            :obj:`tuple` of :obj:`str`: The fully qualified name of the object
        """
        schema_name, table_name = tbl.getSchema(), tbl.getTableName()

        if schema_name is None or schema_name == "":
            schema_name = self.schema_name

        return schema_name, table_name

    def _prepare_schemas(self):
        """
        Create a list of schemas filled with the dataframes
        and functions we have currently in our schema list
        """
        logger.debug(
            f"There are {len(self.schema)} existing schema(s): {self.schema.keys()}"
        )
        schema_list = []

        for schema_name, schema in self.schema.items():
            logger.debug(f"Preparing Schema: '{schema_name}'")
            rust_schema = DaskSchema(schema_name)

            if not schema.tables:
                logger.warning("No tables are registered.")

            for name, dc in schema.tables.items():
                row_count = (
                    float(schema.statistics[name].row_count)
                    if name in schema.statistics
                    else float(0)
                )

                table = DaskTable(schema_name, name, row_count)
                df = dc.df

                for column in df.columns:
                    data_type = df[column].dtype
                    sql_data_type = python_to_sql_type(data_type)
                    table.add_column(column, sql_data_type)

                rust_schema.add_table(table)

            if not schema.functions:
                logger.debug("No custom functions defined.")
            for function_description in schema.function_lists:
                name = function_description.name
                sql_return_type = function_description.return_type
                sql_parameters = function_description.parameters
                if function_description.aggregation:
                    logger.debug(f"Adding function '{name}' to schema as aggregation.")
                    rust_schema.add_or_overload_function(
                        name,
                        [param[1].getDataType() for param in sql_parameters],
                        sql_return_type.getDataType(),
                        True,
                    )
                else:
                    logger.debug(
                        f"Adding function '{name}' to schema as scalar function."
                    )
                    rust_schema.add_or_overload_function(
                        name,
                        [param[1].getDataType() for param in sql_parameters],
                        sql_return_type.getDataType(),
                        False,
                    )

            schema_list.append(rust_schema)

        return schema_list

    def _get_ral(self, sql):
        """Helper function to turn the sql query into a relational algebra and resulting column names"""

        logger.debug(f"Entering _get_ral('{sql}')")

        # get the schema of what we currently have registered
        schemas = self._prepare_schemas()
        for schema in schemas:
            self.context.register_schema(schema.name, schema)
        try:
            sqlTree = self.context.parse_sql(sql)
        except DFParsingException as pe:
            raise ParsingException(sql, str(pe))
        logger.debug(f"_get_ral -> sqlTree: {sqlTree}")

        rel = sqlTree

        # TODO: Need to understand if this list here is actually needed? For now just use the first entry.
        if len(sqlTree) > 1:
            raise RuntimeError(
                f"Multiple 'Statements' encountered for SQL {sql}. Please share this with the dev team!"
            )

        try:
            nonOptimizedRel = self.context.logical_relational_algebra(sqlTree[0])
        except DFParsingException as pe:
            raise ParsingException(sql, str(pe)) from None

        # Optimize the `LogicalPlan` or skip if configured
        if dask_config.get("sql.optimize"):
            try:
                rel = self.context.optimize_relational_algebra(nonOptimizedRel)
            except DFOptimizationException as oe:
                rel = nonOptimizedRel
                raise OptimizationException(str(oe)) from None
        else:
            rel = nonOptimizedRel

        rel_string = rel.explain_original()
        logger.debug(f"_get_ral -> LogicalPlan: {rel}")
        logger.debug(f"Extracted relational algebra:\n {rel_string}")

        return rel, rel_string

    def _compute_table_from_rel(self, rel: "LogicalPlan", return_futures: bool = True):
        dc = RelConverter.convert(rel, context=self)

        # Optimization might remove some alias projects. Make sure to keep them here.
        select_names = [field for field in rel.getRowType().getFieldList()]

        if rel.get_current_node_type() == "Explain":
            return dc
        if dc is None:
            return

        if select_names:
            # Use FQ name if not unique and simple name if it is unique. If a join contains the same column
            # names the output col is prepended with the fully qualified column name
            field_counts = Counter([field.getName() for field in select_names])
            select_names = [
                field.getQualifiedName()
                if field_counts[field.getName()] > 1
                else field.getName()
                for field in select_names
            ]

            cc = dc.column_container
            cc = cc.rename(
                {
                    df_col: select_name
                    for df_col, select_name in zip(cc.columns, select_names)
                }
            )
            dc = DataContainer(dc.df, cc)

        df = dc.assign()
        if not return_futures:
            df = df.compute()

        return df

    def _get_tables_from_stack(self):
        """Helper function to return all dask/pandas dataframes from the calling stack"""
        stack = inspect.stack()

        tables = {}

        # Traverse the stacks from inside to outside
        for frame_info in stack:
            for var_name, variable in frame_info.frame.f_locals.items():
                if var_name.startswith("_"):
                    continue
                if not dd.utils.is_dataframe_like(variable):
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
        row_udf: bool = False,
    ):
        """Helper function to do the function or aggregation registration"""

        schema_name = schema_name or self.schema_name
        schema = self.schema[schema_name]

        # validate and cache UDF metadata
        sql_parameters = [
            (name, python_to_sql_type(param_type)) for name, param_type in parameters
        ]
        sql_return_type = python_to_sql_type(return_type)

        if not aggregation:
            f = UDF(f, row_udf, parameters, return_type)
        lower_name = name.lower()
        if lower_name in schema.functions:
            if replace:
                schema.function_lists = list(
                    filter(
                        lambda f: f.name.lower() != lower_name,
                        schema.function_lists,
                    )
                )
                del schema.functions[lower_name]

            elif schema.functions[lower_name] != f:
                raise ValueError(
                    "Registering multiple functions with the same name is only permitted if replace=True"
                )

        schema.function_lists.append(
            FunctionDescription(
                name.upper(), sql_parameters, sql_return_type, aggregation
            )
        )
        schema.function_lists.append(
            FunctionDescription(
                name.lower(), sql_parameters, sql_return_type, aggregation
            )
        )
        schema.functions[lower_name] = f
