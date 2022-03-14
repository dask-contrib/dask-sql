from typing import TYPE_CHECKING

from dask_planner.rust import (
    DaskFunction,
    DaskSchema,
    DaskTable,
    Query,
    Statement,
    sql_functions,
    LogicalPlan,
)

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql


class DaskTableScanPlugin(BaseRelPlugin):
    """
    A DaskTableScal is the main ingredient: it will get the data
    from the database. It is always used, when the SQL looks like

        SELECT .... FROM table ....

    We need to get the dask dataframe from the registered
    tables and return the requested columns from it.
    Calcite will always refer to columns via index.
    """

    class_name = "com.dask.sql.nodes.DaskTableScan"

    def convert(
        self, rel: LogicalPlan, context: "dask_sql.Context"
    ) -> DataContainer:
        # There should not be any input. This is the first step.
        # self.assert_inputs(rel, 0)
        print(f"Entering table_scan plugin to load data into dataframes")

        print(f"table_scan.convert() -> rel: {rel}")
        field_names = rel.getFieldNames()
        print(f'FIELD_NAMES!!!! {field_names}')

        # The table(s) we need to return
        table = rel.getTable()

        print(f"table_scan.convert() -> table: {table}")

        # The table names are all names split by "."
        # We assume to always have the form something.something
        table_names = [str(n) for n in table.getQualifiedName()]
        assert len(table_names) == 2
        schema_name = table_names[0]
        table_name = table_names[1]
        table_name = table_name.lower()

        print(f"table_scan.convert() -> schema_name: {schema_name} - table_name: {table_name}")

        dc = context.schema[schema_name].tables[table_name]
        df = dc.df
        cc = dc.column_container

        # Make sure we only return the requested columns
        row_type = table.getRowType()
        field_specifications = [str(f) for f in row_type.getFieldNames()]
        print(f"table_scan.convert() -> field_specifications: {field_specifications}")
        cc = cc.limit_to(field_specifications)

        # cc = self.fix_column_to_row_type(cc, rel.getRowType())
        # dc = DataContainer(df, cc)
        # dc = self.fix_dtype_to_row_type(dc, rel.getRowType())
        cc = self.fix_column_to_row_type(cc, table.getRowType())
        dc = DataContainer(df, cc)
        dc = self.fix_dtype_to_row_type(dc, table.getRowType())
        return dc
