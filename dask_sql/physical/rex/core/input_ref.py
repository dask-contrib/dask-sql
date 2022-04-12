from typing import TYPE_CHECKING

import dask.dataframe as dd

from dask_planner.rust import Expression, LogicalPlan
from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rex.base import BaseRexPlugin

if TYPE_CHECKING:
    import dask_sql


class RexInputRefPlugin(BaseRexPlugin):
    """
    A RexInputRef is an expression, which references a single column.
    It is typically to be found in any expressions which
    calculate a function in a column of a table.
    """

    class_name = "InputRef"

    def convert(
        self, rel: LogicalPlan, expr: Expression, dc: DataContainer, context: "dask_sql.Context",
    ) -> dd.Series:
        df = dc.df

        # The column is references by index
        column_name = str(expr.column_name(rel))
        return df[column_name]
