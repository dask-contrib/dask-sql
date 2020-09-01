from typing import Dict, List

import dask.dataframe as dd
from dask.highlevelgraph import HighLevelGraph
from dask.dataframe.core import new_dd_object
import pandas as pd

from dask_sql.physical.rex import RexConverter
from dask_sql.physical.rel.base import BaseRelPlugin


class LogicalSortPlugin(BaseRelPlugin):
    """
    LogicalSort is used to sort by columns (ORDER BY)
    as well as to only get a certain part of the dataframe
    (LIMIT).
    """

    class_name = "org.apache.calcite.rel.logical.LogicalSort"

    def convert(
        self, rel: "org.apache.calcite.rel.RelNode", tables: Dict[str, dd.DataFrame]
    ) -> dd.DataFrame:
        (df,) = self.assert_inputs(rel, 1, tables)
        self.check_columns_from_row_type(df, rel.getExpectedInputRowType(0))

        sort_collation = rel.getCollation().getFieldCollations()
        sort_columns = [df.columns[int(x.getFieldIndex())] for x in sort_collation]
        sort_ascending = [str(x.getDirection()) == "ASCENDING" for x in sort_collation]

        offset = rel.offset
        if offset:
            offset = RexConverter.convert(offset, df)

        end = rel.fetch
        if end:
            end = RexConverter.convert(end, df)

            if offset:
                end += offset

        if sort_columns:
            df = self._apply_sort(df, sort_columns, sort_ascending)

        if offset is not None or end is not None:
            df = self._apply_offset(df, offset, end)

        df = self.fix_column_to_row_type(df, rel.getRowType())
        return df

    def _apply_sort(
        self, df: dd.DataFrame, sort_columns: List[str], sort_ascending: List[bool]
    ) -> dd.DataFrame:
        # Split the first column. We need to handle this one with set_index
        first_sort_column = sort_columns[0]
        first_sort_ascending = sort_ascending[0]

        # Sort the first column with set_index. Currently, we can only handle ascending sort
        if not first_sort_ascending:
            raise NotImplementedError(
                "The first column needs to be sorted ascending (yet)"
            )
        # We can only sort if there are no NaNs or infs.
        # Therefore we need to do a single pass over the dataframe
        # to warn the user
        # We shall also treat inf as na
        with pd.option_context("use_inf_as_na", True):
            if not (~df[first_sort_column].isna()).all().compute():
                raise ValueError("Can not sort a column with NaNs")

        df = df.set_index(first_sort_column, drop=False).reset_index(drop=True)

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
        calculate (!!!) the sizes of each partition
        (this means we need to compute the dataframe already here).

        After that, we can create a new dataframe from the old
        dataframe by calculating for each partition if and how much
        it should be used.
        We do this via generating our own dask computation graph as
        we need to pass the partition number to the selection
        function, which is not possible with normal "map_partitions".
        """
        # As we need to calculate the partition size, we better persist
        # the df. I think...
        # TODO: check if this is the best thing to do
        df = df.persist()

        # First, we need to find out which partitions we want to use.
        # Therefore we count the total number of entries
        partition_borders = df.map_partitions(lambda x: len(x)).compute()
        partition_borders = partition_borders.cumsum().to_dict()

        # Now we let each of the partitions figure out, how much it needs to return
        # using these partition borders
        # For this, we generate out own dask computation graph (as it does not really)
        # fit well with one of the already present methods

        # (a) we define a method to be calculated on each partition
        # This method returns the part of the partition, which falls between [offset, fetch]
        def select_from_to(df, partition_index):
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

        # Then we (b) define a task graph. It should calculate the function above on each of the partitions of
        # df (specified by (df._name, i) for each partition i). As an argument, we pass the partition_index.
        dask_graph_name = df._name + "-limit"
        dask_graph_dict = {}

        for partition_index in range(df.npartitions):
            dask_graph_dict[(dask_graph_name, partition_index)] = (
                select_from_to,
                (df._name, partition_index),
                partition_index,
            )

        # We replace df with our new graph
        graph = HighLevelGraph.from_collections(
            dask_graph_name, dask_graph_dict, dependencies=[df]
        )
        return new_dd_object(graph, dask_graph_name, df._meta, df.divisions)
