from typing import TYPE_CHECKING

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.rex import RexConverter

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan


class DaskLimitPlugin(BaseRelPlugin):
    """
    Limit is used to only get a certain part of the dataframe
    (LIMIT).
    """

    class_name = "Limit"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        (dc,) = self.assert_inputs(rel, 1, context)
        df = dc.df
        cc = dc.column_container

        limit = RexConverter.convert(rel, rel.limit().getLimitN(), df, context=context)

        # If an offset was present it would have already been processed at this point.
        # Therefore it is always safe to start at 0 when applying the limit
        df = df.head(limit, npartitions=-1, compute=False)

        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        # No column type has changed, so no need to cast again
        return DataContainer(df, cc)
