import logging
from typing import TYPE_CHECKING

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan

logger = logging.getLogger(__name__)


class DaskTableScanPlugin(BaseRelPlugin):
    """
    A DaskTableScal is the main ingredient: it will get the data
    from the database. It is always used, when the SQL looks like

        SELECT .... FROM table ....

    We need to get the dask dataframe from the registered
    tables and return the requested columns from it.
    Calcite will always refer to columns via index.
    """

    class_name = "TableScan"

    def convert(
        self,
        rel: "LogicalPlan",
        context: "dask_sql.Context",
    ) -> DataContainer:
        # There should not be any input. This is the first step.
        self.assert_inputs(rel, 0)

        # Rust table_scan instance handle
        table_scan = rel.table_scan()

        # The table(s) we need to return
        table = rel.getTable()

        # The table names are all names split by "."
        # We assume to always have the form something.something
        table_names = [str(n) for n in table.getQualifiedName(rel)]
        assert len(table_names) == 2
        schema_name = table_names[0]
        table_name = table_names[1]
        table_name = table_name.lower()

        dc = context.schema[schema_name].tables[table_name]
        df = dc.df
        cc = dc.column_container

        # If the 'TableScan' instance contains projected columns only retrieve those columns
        # otherwise get all projected columns from the 'Projection' instance, which is contained
        # in the 'RelDataType' instance, aka 'row_type'
        if table_scan.containsProjections():
            field_specifications = (
                table_scan.getTableScanProjects()
            )  # Assumes these are column projections only and field names match table column names
            df = df[field_specifications]
        else:
            field_specifications = [str(f) for f in table.getRowType().getFieldNames()]

        cc = cc.limit_to(field_specifications)
        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        dc = DataContainer(df, cc)
        dc = self.fix_dtype_to_row_type(dc, rel.getRowType())
        return dc
