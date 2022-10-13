from typing import TYPE_CHECKING

import dask.dataframe as dd
from dask import config as dask_config
from dask.blockwise import Blockwise
from dask.highlevelgraph import MaterializedLayer
from dask.layers import DataFrameIOLayer

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

        # Retrieve the RexType::Literal values from the `LogicalPlan` Limit
        # Fetch -> LIMIT
        # Skip -> OFFSET
        limit = RexConverter.convert(rel, rel.limit().getFetch(), df, context=context)
        offset = RexConverter.convert(rel, rel.limit().getSkip(), df, context=context)

        # apply offset to limit if specified
        if limit and offset:
            limit += offset

        # apply limit and/or offset to DataFrame
        df = self._apply_limit(df, limit, offset)
        cc = self.fix_column_to_row_type(cc, rel.getRowType())

        # No column type has changed, so no need to cast again
        return DataContainer(df, cc)

    def _apply_limit(self, df: dd.DataFrame, limit: int, offset: int) -> dd.DataFrame:
        """
        Limit the dataframe to the window [offset, limit].

        Unfortunately, Dask does not currently support row selection through `iloc`, so this must be done using a custom partition function.
        However, it is sometimes possible to compute this window using `head` when an `offset` is not specified.
        """
        # if no offset is specified we can use `head` to compute the window
        if not offset:
            # if `check-first-partition` enabled, check if we have a relatively simple Dask graph and if so,
            # check if the first partition contains our desired window
            if (
                dask_config.get("sql.limit.check-first-partition")
                and all(
                    [
                        isinstance(
                            layer, (DataFrameIOLayer, Blockwise, MaterializedLayer)
                        )
                        for layer in df.dask.layers.values()
                    ]
                )
                and limit <= len(df.partitions[0])
            ):
                return df.head(limit, compute=False)

            return df.head(limit, npartitions=-1, compute=False)

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

            partition_border_left = (
                partition_borders[partition_index - 1] if partition_index > 0 else 0
            )
            partition_border_right = partition_borders[partition_index]

            if (limit and limit < partition_border_left) or (
                offset >= partition_border_right
            ):
                return df.iloc[0:0]

            from_index = max(offset - partition_border_left, 0)
            to_index = (
                min(limit, partition_border_right) if limit else partition_border_right
            ) - partition_border_left

            return df.iloc[from_index:to_index]

        return df.map_partitions(
            limit_partition_func,
            partition_borders=partition_borders,
        )
