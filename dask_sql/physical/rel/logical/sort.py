from dask_sql.utils import new_temporary_column
from typing import List

import dask
import dask.dataframe as dd

from dask_sql.physical.rex import RexConverter
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import DataContainer
from dask_sql.java import org


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

        if sort_columns:
            ASCENDING = org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING
            FIRST = org.apache.calcite.rel.RelFieldCollation.NullDirection.FIRST
            sort_ascending = [x.getDirection() == ASCENDING for x in sort_collation]
            sort_null_first = [x.nullDirection == FIRST for x in sort_collation]

            df = self._apply_sort(df, sort_columns, sort_ascending, sort_null_first)

        offset = rel.offset
        if offset:
            offset = RexConverter.convert(offset, df, context=context)

        end = rel.fetch
        if end:
            end = RexConverter.convert(end, df, context=context)

            if offset:
                end += offset

        if offset is not None or end is not None:
            df = self._apply_offset(df, offset, end)

        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        # No column type has changed, so no need to cast again
        return DataContainer(df, cc)

    def _apply_sort(
        self,
        df: dd.DataFrame,
        sort_columns: List[str],
        sort_ascending: List[bool],
        sort_null_first: List[bool],
    ) -> dd.DataFrame:
        # Split the first column. We need to handle this one with set_index
        first_sort_column = sort_columns[0]
        first_sort_ascending = sort_ascending[0]
        first_null_first = sort_null_first[0]

        # Only sort by first column first
        df = self._sort_first_column(
            df, first_sort_column, first_sort_ascending, first_null_first
        )

        # sort the remaining columns if given
        if len(sort_columns) > 1:
            df = self._sort_secondary_columns(
                df, sort_columns, sort_ascending, sort_null_first
            )

        return df.persist()

    def _sort_secondary_columns(
        self, df, sort_columns, sort_ascending, sort_null_first
    ):
        def sort_partition_func(partition):
            # We are going to add additional columns here
            # That is not something we would like to do on the
            # original data. I hope that this does not have
            # a huge performance impact
            partition = partition.copy()

            # pandas does not allow to sort by NaN first/last
            # differently for different columns. Therefore
            # we split again by NaN
            original_columns = partition.columns
            tmp_columns = []
            tmp_ascending = []

            for col, asc, null_first in zip(
                sort_columns, sort_ascending, sort_null_first
            ):
                is_null_col = new_temporary_column(partition)
                partition[is_null_col] = partition[col].isna()
                tmp_columns += [is_null_col]

                if null_first:
                    tmp_ascending += [False]
                else:
                    tmp_ascending += [True]

                filled_nan_col = new_temporary_column(partition)
                # the actual value does not matter
                partition[filled_nan_col] = partition[col].fillna(0)
                tmp_columns += [filled_nan_col]
                tmp_ascending += [asc]

            partition = partition.sort_values(tmp_columns, ascending=tmp_ascending)
            return partition[original_columns]

        return df.map_partitions(sort_partition_func, meta=df._meta)

    def _sort_first_column(
        self, df, first_sort_column, first_sort_ascending, first_null_first
    ):
        # Dask can only sort if there are no NaNs in the first column.
        # Therefore we need to do a single pass over the dataframe
        # to check if we have NaNs in the first column
        # If this is the case, we concat the NaN values to the front
        # That might be a very complex operation and should
        # in general be avoided
        col = df[first_sort_column]
        is_na = col.isna().persist()
        if is_na.any().compute():
            df_is_na = df[is_na].reset_index(drop=True).repartition(1)
            df_not_is_na = (
                df[~is_na]
                .set_index(first_sort_column, drop=False)
                .reset_index(drop=True)
            )
        else:
            df_is_na = None
            df_not_is_na = df.set_index(first_sort_column, drop=False).reset_index(
                drop=True
            )
        if not first_sort_ascending:
            # As set_index().reset_index() always sorts ascending, we need to reverse
            # the order inside all partitions and the order of the partitions itself
            # We do not need to do this for the nan-partitions
            df_not_is_na = df_not_is_na.map_partitions(
                lambda partition: partition[::-1], meta=df
            )
            df_not_is_na = df_not_is_na.partitions[::-1]

        if df_is_na is not None:
            if first_null_first:
                df = dd.concat([df_is_na, df_not_is_na])
            else:
                df = dd.concat([df_not_is_na, df_is_na])
        else:
            df = df_not_is_na

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
            # is already enough, we will just use this
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
