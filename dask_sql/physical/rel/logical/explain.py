from typing import TYPE_CHECKING

from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    from dask_planner import LogicalPlan

    import dask_sql


class ExplainPlugin(BaseRelPlugin):
    """
    Explain is used to explain the query with the EXPLAIN keyword
    """

    class_name = "Explain"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context"):
        explain_strings = rel.explain().getExplainString()
        return "\n".join(explain_strings)
