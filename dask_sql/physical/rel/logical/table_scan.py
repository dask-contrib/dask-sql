import logging
import operator
from functools import reduce
from typing import TYPE_CHECKING

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.rel.logical.filter import filter_or_scalar
from dask_sql.physical.rex import RexConverter

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan

logger = logging.getLogger(__name__)


class DaskTableScanPlugin(BaseRelPlugin):
    """
    A DaskTableScan is the main ingredient: it will get the data
    from the database. It is always used, when the SQL looks like

        SELECT .... FROM table ....

    We need to get the dask dataframe from the registered
    tables and return the requested columns from it.
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
        dask_table = rel.getTable()
        schema_name, table_name = [n.lower() for n in context.fqn(dask_table)]

        dc = context.schema[schema_name].tables[table_name]

        # Apply filter before projections since filter columns may not be in projections
        dc = self._apply_filters(table_scan, rel, dc, context)
        dc = self._apply_projections(table_scan, dask_table, dc)

        cc = dc.column_container
        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        dc = DataContainer(dc.df, cc)
        dc = self.fix_dtype_to_row_type(dc, rel.getRowType())
        return dc

    def _apply_projections(self, table_scan, dask_table, dc):
        # If the 'TableScan' instance contains projected columns only retrieve those columns
        # otherwise get all projected columns from the 'Projection' instance, which is contained
        # in the 'RelDataType' instance, aka 'row_type'
        df = dc.df
        cc = dc.column_container
        if table_scan.containsProjections():
            field_specifications = (
                table_scan.getTableScanProjects()
            )  # Assumes these are column projections only and field names match table column names
            df = df[field_specifications]
        else:
            field_specifications = [
                str(f) for f in dask_table.getRowType().getFieldNames()
            ]
        cc = cc.limit_to(field_specifications)
        return DataContainer(df, cc)

    def _apply_filters(self, table_scan, rel, dc, context):
        df = dc.df
        cc = dc.column_container
        filters = table_scan.getFilters()
        # All partial filters here are applied in conjunction (&)
        if filters:
            df_condition = reduce(
                operator.and_,
                [
                    RexConverter.convert(rel, rex, dc, context=context)
                    for rex in filters
                ],
            )
            df = filter_or_scalar(df, df_condition)

        return DataContainer(df, cc)
