from typing import TYPE_CHECKING

import dask.dataframe as dd

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.rex import RexConverter

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan


class DaskOffsetPlugin(BaseRelPlugin):
    """
    Offset is used to modify the effective expression bounds in a larger table
    (OFFSET).
    """

    class_name = "Offset"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        (dc,) = self.assert_inputs(rel, 1, context)
        df = dc.df
        cc = dc.column_container

        offset = RexConverter.convert(
            rel, rel.offset().getOffset(), df, context=context
        )

        df = self._apply_offset(df, offset)

        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        # No column type has changed, so no need to cast again
        return DataContainer(df, cc)

    def _apply_offset(self, df: dd.DataFrame, offset: int) -> dd.DataFrame:
        """
        Limit the dataframe to the window [offset, end].

        Unfortunately, Dask does not currently support row selection through `iloc`, so this must be done using a custom partition function.
        However, it is sometimes possible to compute this window using `head` when an `offset` is not specified.
        """
        # compute the size of each partition
        # TODO: compute `cumsum` here when dask#9067 is resolved
        partition_borders = df.map_partitions(lambda x: len(x))

        def offset_partition_func(df, partition_borders, partition_info=None):
            """Limit the partition to values contained within the specified window, returning an empty dataframe if there are none"""

            # TODO: remove the `cumsum` call here when dask#9067 is resolved
            partition_borders = partition_borders.cumsum().to_dict()
            partition_index = (
                partition_info["number"] if partition_info is not None else 0
            )

            partition_border_left = (
                partition_borders[partition_index - 1] if partition_index > 0 else 0
            )
            partition_border_right = partition_borders[partition_index]

            if offset >= partition_border_right:
                return df.iloc[0:0]

            from_index = max(offset - partition_border_left, 0)

            return df.iloc[from_index:]

        return df.map_partitions(
            offset_partition_func,
            partition_borders=partition_borders,
        )
