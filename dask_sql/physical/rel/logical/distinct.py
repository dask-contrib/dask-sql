import logging
from typing import TYPE_CHECKING

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan

logger = logging.getLogger(__name__)


class DaskDistinctPlugin(BaseRelPlugin):
    """
    Eliminates all duplicate entries from the specified DataFrame column
    """

    class_name = "Distinct"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        (dc,) = self.assert_inputs(rel, 1, context)
        df = dc.df
        cc = dc.column_container

        df = df.drop_duplicates()

        return DataContainer(df, cc)
