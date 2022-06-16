from typing import TYPE_CHECKING

import dask.dataframe as dd

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel import RelConverter
from dask_sql.physical.rex.base import BaseRexPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import Expression, LogicalPlan


class RexSubqueryAliasPlugin(BaseRexPlugin):
    """
    A RexSubqueryAliasPlugin is an expression, which references a Subquery.
    This plugin is thin on logic, however keeping with previous patterns
    we use the plugin approach instead of placing the logic inline
    """

    class_name = "SubqueryAlias"

    def convert(
        self,
        rel: "LogicalPlan",
        rex: "Expression",
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> dd.DataFrame:

        # Extract the LogicalPlan from the Expr instance
        sub_rel = rex.getSubqueryLogicalPlan()

        dc = RelConverter.convert(sub_rel, context=context)
        return dc.df
