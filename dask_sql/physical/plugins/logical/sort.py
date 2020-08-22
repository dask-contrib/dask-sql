from dask.highlevelgraph import HighLevelGraph
from dask.dataframe.core import new_dd_object

from dask_sql.physical.ral import (
    convert_ral_to_df,
    fix_column_to_row_type,
    check_columns_from_row_type,
)
from dask_sql.physical.rex import apply_rex_call


class LogicalSortPlugin:
    class_name = "org.apache.calcite.rel.logical.LogicalSort"

    def __call__(self, ral, tables):
        assert len(ral.getInputs()) == 1

        input_ral = ral.getInput()
        df = convert_ral_to_df(input_ral, tables)
        check_columns_from_row_type(df, ral.getExpectedInputRowType(0))

        sort_collation = ral.getCollation().getFieldCollations()
        sort_columns = [df.columns[int(x.getFieldIndex())] for x in sort_collation]
        sort_ascending = [str(x.getDirection()) == "ASCENDING" for x in sort_collation]

        # The LogicalSort is also used on LIMIT clauses
        offset = ral.offset
        if offset:
            offset = apply_rex_call(offset, df)

        end = None
        fetch = ral.fetch
        if fetch:
            end = apply_rex_call(fetch, df)

            if offset:
                end += offset

        # TODO: check if this is the best thing to do
        df = df.persist()

        # The LogicalSort is sometimes also used, if there is actually no sort involved
        if sort_columns:
            # Split the first column. We need to handle this one with set_index
            first_sort_column = sort_columns[0]
            first_sort_ascending = sort_ascending[0]

            # Sort the first column with set_index. Currently, we can only handle ascending sort
            if not first_sort_ascending:
                raise NotImplementedError(
                    "The first column needs to be sorted ascending (yet)"
                )
            df = df.set_index(first_sort_column, drop=False).reset_index(drop=True)

            # sort the remaining columns if given
            if len(sort_columns) > 1:
                sort_parition_func = lambda x: x.reset_index(drop=True).sort_values(
                    sort_columns, ascending=sort_ascending
                )
                df = df.map_partitions(sort_parition_func, meta=df._meta)

        # Apply the fetch and offset limits if given
        if offset is not None or end is not None:
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

                from_index = (
                    max(offset - this_partition_border_left, 0) if offset else 0
                )
                to_index = (
                    min(end, this_partition_border_right)
                    if end
                    else this_partition_border_right
                ) - this_partition_border_left

                return df.iloc[from_index:to_index]

            # Then we define a task graph. It should calculate the function above on each of the partitions of
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
            df = new_dd_object(graph, dask_graph_name, df._meta, df.divisions)

        df = fix_column_to_row_type(df, ral.getRowType())

        return df
