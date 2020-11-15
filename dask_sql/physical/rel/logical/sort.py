from typing import List

import dask
import dask.dataframe as dd
import pandas as pd
import dask.array as da

from dask_sql.physical.rex import RexConverter
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import DataContainer


class LogicalSortPlugin(BaseRelPlugin):
    """
    LogicalSort is used to sort by columns (ORDER BY)
    as well as to only get a certain part of the dataframe
    (LIMIT).
    """

    class_name = "org.apache.calcite.rel.logical.LogicalSort"

    def convert(
        self, rel: "org.apache.calcite.rel.RelNode", context: "dask_sql.Context"
    ) -> DataContainer:
        (dc,) = self.assert_inputs(rel, 1, context)
        df = dc.df
        cc = dc.column_container

        sort_collation = rel.getCollation().getFieldCollations()
        sort_columns = [
            cc.get_backend_by_frontend_index(int(x.getFieldIndex()))
            for x in sort_collation
        ]
        sort_ascending = [str(x.getDirection()) == "ASCENDING" for x in sort_collation]

        offset = rel.offset
        if offset:
            offset = RexConverter.convert(offset, df, context=context)

        end = rel.fetch
        if end:
            end = RexConverter.convert(end, df, context=context)

            if offset:
                end += offset

        if sort_columns:
            df = self._apply_sort(df, sort_columns, sort_ascending)

        if offset is not None or end is not None:
            df = self._apply_offset(df, offset, end)

        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        # No column type has changed, so no need to cast again
        return DataContainer(df, cc)

    def _apply_sort(
        self, df: dd.DataFrame, sort_columns: List[str], sort_ascending: List[bool]
    ) -> dd.DataFrame:
        # Split the first column. We need to handle this one with set_index
        first_sort_column = sort_columns[0]
        first_sort_ascending = sort_ascending[0]

        # We can only sort if there are no NaNs or infs.
        # Therefore we need to do a single pass over the dataframe
        # to warn the user
        # We shall also treat inf as na
        col = df[first_sort_column]
        isnan = col.isna().any()
        numeric = pd.api.types.is_numeric_dtype(col.dtype)
        # Important to evaluate the isinf late, as it only works with numeric-type columns
        if isnan.compute() or (numeric and da.isinf(col).any().compute()):
            raise ValueError("Can not sort a column with NaNs")

        df = df.set_index(first_sort_column, drop=False).reset_index(drop=True)
        if not first_sort_ascending:
            # As set_index().reset_index() always sorts ascending, we need to reverse
            # the order inside all partitions and the order of the partitions itself
            df = df.map_partitions(lambda partition: partition[::-1], meta=df)
            df = df.partitions[::-1]

        # sort the remaining columns if given
        if len(sort_columns) > 1:
            sort_partition_func = lambda x: x.reset_index(drop=True).sort_values(
                sort_columns, ascending=sort_ascending
            )
            df = df.map_partitions(sort_partition_func, meta=df._meta)

        return df

    def _apply_offset(self, df: dd.DataFrame, offset: int, end: int) -> dd.DataFrame:
        """
        Limit the dataframe to the window [offset, end].
        That is unfortunately, not so simple as we do not know how many
        items we have in each partition. We have therefore no other way than to
        calculate (!!!) the sizes of each partition.

        After that, we can create a new dataframe from the old
        dataframe by calculating for each partition if and how much
        it should be used.
        We do this via generating our own dask computation graph as
        we need to pass the partition number to the selection
        function, which is not possible with normal "map_partitions".
        """
        if not offset:
            # We do a (hopefully) very quick check: if the first partition
            # is already enough, we will just ust this
            first_partition_length = len(df.partitions[0])
            if first_partition_length >= end:
                return df.head(end, compute=False)

        # First, we need to find out which partitions we want to use.
        # Therefore we count the total number of entries
        partition_borders = df.map_partitions(lambda x: len(x))

        # Now we let each of the partitions figure out, how much it needs to return
        # using these partition borders
        # For this, we generate out own dask computation graph (as it does not really
        # fit well with one of the already present methods).

        # (a) we define a method to be calculated on each partition
        # This method returns the part of the partition, which falls between [offset, fetch]
        # Please note that the dask object "partition_borders", will be turned into
        # its pandas representation at this point and we can calculate the cumsum
        # (which is not possible on the dask object). Recalculating it should not cost
        # us much, as we assume the number of partitions is rather small.
        @dask.delayed
        def select_from_to(df, partition_index, partition_borders):
            partition_borders = partition_borders.cumsum().to_dict()
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

        # (b) Now we just need to apply the function on every partition
        # We do this via the delayed interface, which seems the easiest one.
        return dd.from_delayed(
            [
                select_from_to(partition, partition_number, partition_borders)
                for partition_number, partition in enumerate(df.partitions)
            ]
        )
