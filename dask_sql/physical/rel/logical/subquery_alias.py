from typing import TYPE_CHECKING

from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan


class SubqueryAlias(BaseRelPlugin):
    """
    SubqueryAlias is used to assign an alias to a table and/or subquery
    """

    class_name = "SubqueryAlias"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context"):
        (dc,) = self.assert_inputs(rel, 1, context)
        return dc
