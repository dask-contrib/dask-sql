from typing import List

import dask.dataframe as dd

from dask_sql.utils import new_temporary_column


def apply_sort(
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
    df = _sort_first_column(
        df, first_sort_column, first_sort_ascending, first_null_first
    )

    # sort the remaining columns if given
    if len(sort_columns) > 1:
        df = df.map_partitions(
            sort_partition_func,
            meta=df._meta,
            sort_columns=sort_columns,
            sort_ascending=sort_ascending,
            sort_null_first=sort_null_first,
        )

    return df.persist()


def sort_partition_func(partition, sort_columns, sort_ascending, sort_null_first):
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

    for col, asc, null_first in zip(sort_columns, sort_ascending, sort_null_first):
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


def _sort_first_column(df, first_sort_column, first_sort_ascending, first_null_first):
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
            df[~is_na].set_index(first_sort_column, drop=False).reset_index(drop=True)
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
