from typing import TYPE_CHECKING

from dask_planner.rust import py_column_name, row_type, sort_ascending, sort_nulls_first
from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.utils.sort import apply_sort

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import DaskLogicalPlan


class DaskSortPlugin(BaseRelPlugin):
    """
    DaskSort is used to sort by columns (ORDER BY).
    """

    class_name = "Sort"

    def convert(
        self, rel: "DaskLogicalPlan", context: "dask_sql.Context"
    ) -> DataContainer:
        (dc,) = self.assert_inputs(rel, 1, context)
        df = dc.df
        cc = dc.column_container
        sort_plan = rel.to_variant()
        sort_expressions = sort_plan.getCollation()
        sort_columns = [
            cc.get_backend_by_frontend_name(py_column_name(expr, rel))
            for expr in sort_expressions
        ]
        sort_ascending_exprs = [sort_ascending(expr) for expr in sort_expressions]
        sort_null_first = [sort_nulls_first(expr) for expr in sort_expressions]
        sort_num_rows = sort_plan.getNumRows()

        df = apply_sort(
            df, sort_columns, sort_ascending_exprs, sort_null_first, sort_num_rows
        )

        cc = self.fix_column_to_row_type(cc, row_type(rel))
        # No column type has changed, so no need to cast again
        return DataContainer(df, cc)
