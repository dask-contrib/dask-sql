from typing import TYPE_CHECKING

import dask.dataframe as dd

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.rex import RexConverter

if TYPE_CHECKING:
    import dask_sql
    from dask_sql.java import org


class DaskLimitPlugin(BaseRelPlugin):
    """
    Limit is used to only get a certain part of the dataframe
    (LIMIT).
    """

    class_name = "com.dask.sql.nodes.DaskLimit"

    def convert(
        self, rel: "org.apache.calcite.rel.RelNode", context: "dask_sql.Context"
    ) -> DataContainer:
        (dc,) = self.assert_inputs(rel, 1, context)
        df = dc.df
        cc = dc.column_container

        offset = rel.getOffset()
        if offset:
            offset = RexConverter.convert(offset, df, context=context)

        end = rel.getFetch()
        if end:
            end = RexConverter.convert(end, df, context=context)

            if offset:
                end += offset

        df = self._apply_limit(df, offset, end)

        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        # No column type has changed, so no need to cast again
        return DataContainer(df, cc)

    def _apply_limit(self, df: dd.DataFrame, offset: int, end: int) -> dd.DataFrame:
        """
        Limit the dataframe to the window [offset, end].

        Unfortunately, Dask does not currently support row selection through `iloc`, so this must be done using a custom partition function.
        However, it is sometimes possible to compute this window using `head` when an `offset` is not specified.
        """
        if not offset:
            # We do a (hopefully) very quick check: if the first partition
            # is already enough, we will just use this
            first_partition_length = len(df.partitions[0])
            if first_partition_length >= end:
                return df.head(end, compute=False)

        # compute the size of each partition
        # TODO: compute `cumsum` here when dask#9067 is resolved
        partition_borders = df.map_partitions(lambda x: len(x))

        def limit_partition_func(df, partition_borders, partition_info=None):
            """Limit the partition to values contained within the specified window, returning an empty dataframe if there are none"""

            # TODO: remove the `cumsum` call here when dask#9067 is resolved
            partition_borders = partition_borders.cumsum().to_dict()
            partition_index = (
                partition_info["number"] if partition_info is not None else 0
            )

            this_partition_border_left = (
                partition_borders[partition_index - 1] if partition_index > 0 else 0
            )
            this_partition_border_right = partition_borders[partition_index]

            if (end and end < this_partition_border_left) or (
                offset and offset >= this_partition_border_right
            ):
                return df.iloc[0:0]

            from_index = max(offset - this_partition_border_left, 0) if offset else 0
            to_index = (
                min(end, this_partition_border_right)
                if end
                else this_partition_border_right
            ) - this_partition_border_left

            return df.iloc[from_index:to_index]

        return df.map_partitions(
            limit_partition_func,
            partition_borders=partition_borders,
        )
