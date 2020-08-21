from dask_sql.java import DaskSchema, DaskTable, RelationalAlgebraGenerator
from dask_sql.mappings import NP_TO_SQL
from dask_sql.physical.rex import register_plugin_class as rex_register_plugin_class
from dask_sql.physical.ral import register_plugin_class as ral_register_plugin_class
from dask_sql.physical.plugins import logical
from dask_sql.physical.plugins import rex
from dask_sql.physical.ral import convert_ral_to_df


class Context:
    def __init__(self):
        self.tables = {}

        ral_register_plugin_class(logical.LogicalTableScanPlugin)
        ral_register_plugin_class(logical.LogicalFilterPlugin)
        ral_register_plugin_class(logical.LogicalProjectPlugin)
        ral_register_plugin_class(logical.LogicalJoinPlugin)

        rex_register_plugin_class(rex.RexInputRefPlugin)
        rex_register_plugin_class(rex.RexCallPlugin)
        rex_register_plugin_class(rex.RexLiteralPlugin)

    def register_dask_table(self, df, name):
        self.tables[name] = df.copy()

    def _get_ral(self, sql):
        # Create a schema filled with the dataframes we have
        # currently in our list
        schema = DaskSchema("schema")

        for name, df in self.tables.items():
            table = DaskTable(name)
            for order, column in enumerate(df.columns):
                data_type = df[column].dtype
                sql_data_type = NP_TO_SQL[data_type]

                table.addColumn(column, sql_data_type)

            schema.addTable(table)

        # Now create a relational algebra from that
        generator = RelationalAlgebraGenerator(schema)
        # TODO: Debug
        print(generator.getRelationalAlgebraString(sql))
        ral = generator.getRelationalAlgebra(sql)

        return ral

    def sql(self, sql):
        ral = self._get_ral(sql)
        df = convert_ral_to_df(ral, tables=self.tables)
        return df
