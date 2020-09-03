import dask.dataframe as dd

from dask_sql.java import DaskSchema, DaskTable, RelationalAlgebraGenerator
from dask_sql.mappings import python_to_sql_type
from dask_sql.physical.rel import RelConverter, logical
from dask_sql.physical.rex import RexConverter, core


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
        self.tables[name] = df.copy()

    def sql(self, sql: str, debug: bool = False) -> dd.DataFrame:
        """
        Query the registered tables with the given SQL.
        The SQL follows approximately the MYSQL standard - however, not all
        operations are already implemented.
        In general, only select statements (no data manipulation) works.
        """
        # TODO: show a nice error message if something is broken
        rel = self._get_ral(sql, debug=debug)
        df = RelConverter.convert(rel, tables=self.tables)
        return df

    def _get_ral(self, sql, debug: bool = False):
        """Helper function to turn the sql query into a relational algebra"""
        # Create a schema filled with the dataframes we have
        # currently in our list
        schema = DaskSchema("schema")

        for name, df in self.tables.items():
            table = DaskTable(name)
            for order, column in enumerate(df.columns):
                data_type = df[column].dtype
                sql_data_type = python_to_sql_type(data_type)

                table.addColumn(column, sql_data_type)

            schema.addTable(table)

        # Now create a relational algebra from that
        generator = RelationalAlgebraGenerator(schema)

        rel = generator.getRelationalAlgebra(sql)
        if debug:  # pragma: no cover
            print(generator.getRelationalAlgebraString(rel))

        return rel
