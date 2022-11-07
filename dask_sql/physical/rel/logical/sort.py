from typing import TYPE_CHECKING

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.utils.sort import apply_sort

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan


class DaskSortPlugin(BaseRelPlugin):
    """
    DaskSort is used to sort by columns (ORDER BY).
    """

    class_name = "Sort"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        (dc,) = self.assert_inputs(rel, 1, context)
        df = dc.df
        cc = dc.column_container
        sort_plan = rel.sort()
        sort_expressions = sort_plan.getCollation()
        sort_columns = [
            cc.get_backend_by_frontend_name(expr.column_name(rel))
            for expr in sort_expressions
        ]
        sort_ascending = [expr.isSortAscending() for expr in sort_expressions]
        sort_null_first = [expr.isSortNullsFirst() for expr in sort_expressions]
        sort_num_rows = sort_plan.getNumRows()

        df = apply_sort(
            df, sort_columns, sort_ascending, sort_null_first, sort_num_rows
        )

        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        # No column type has changed, so no need to cast again
        return DataContainer(df, cc)
