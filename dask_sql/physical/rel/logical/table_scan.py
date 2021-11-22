from typing import TYPE_CHECKING

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_sql.java import org


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
        self, rel: "org.apache.calcite.rel.RelNode", context: "dask_sql.Context"
    ) -> DataContainer:
        # There should not be any input. This is the first step.
        self.assert_inputs(rel, 0)

        # The table(s) we need to return
        table = rel.getTable()

        # The table names are all names split by "."
        # We assume to always have the form something.something
        table_names = [str(n) for n in table.getQualifiedName()]
        assert len(table_names) == 2
        schema_name = table_names[0]
        table_name = table_names[1]
        table_name = table_name.lower()

        dc = context.schema[schema_name].tables[table_name]
        df = dc.df
        cc = dc.column_container

        # Make sure we only return the requested columns
        row_type = table.getRowType()
        field_specifications = [str(f) for f in row_type.getFieldNames()]
        cc = cc.limit_to(field_specifications)

        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        dc = DataContainer(df, cc)
        dc = self.fix_dtype_to_row_type(dc, rel.getRowType())
        return dc
