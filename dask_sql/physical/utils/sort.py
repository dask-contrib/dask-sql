from typing import List

import dask.dataframe as dd
import pandas as pd
from dask.utils import M

from dask_sql.utils import make_pickable_without_dask_sql

try:
    import dask_cudf
except ImportError:
    dask_cudf = None


def apply_sort(
    df: dd.DataFrame,
    sort_columns: List[str],
    sort_ascending: List[bool],
    sort_null_first: List[bool],
) -> dd.DataFrame:
    # if we have a single partition, we can sometimes sort with map_partitions
    if df.npartitions == 1 and (all(sort_null_first) or not any(sort_null_first)):
        return df.map_partitions(
            M.sort_values,
            by=sort_columns,
            ascending=sort_ascending,
            na_position="first" if sort_null_first[0] else "last",
        ).persist()

    # dask-cudf only supports ascending sort / nulls last:
    # https://github.com/rapidsai/cudf/pull/9250
    # https://github.com/rapidsai/cudf/pull/9264
    if (
        dask_cudf is not None
        and isinstance(df, dask_cudf.DataFrame)
        and all(sort_ascending)
        and not any(sort_null_first)
    ):
        try:
            return df.sort_values(sort_columns, ignore_index=True).persist()
        except ValueError:
            pass

    # if standard `sort_values` can't handle ascending / null position params,
    # we extend it using our custom sort function
    return df.sort_values(
        by=sort_columns[0],
        ascending=sort_ascending[0],
        na_position="first" if sort_null_first[0] else "last",
        sort_function=make_pickable_without_dask_sql(sort_partition_func),
        sort_function_kwargs={
            "sort_columns": sort_columns,
            "sort_ascending": sort_ascending,
            "sort_null_first": sort_null_first,
        },
    ).persist()


def sort_partition_func(
    partition: pd.DataFrame,
    sort_columns: List[str],
    sort_ascending: List[bool],
    sort_null_first: List[bool],
    **kwargs,
):
    if partition.empty:
        return partition

    # Trick: https://github.com/pandas-dev/pandas/issues/17111
    # to make sorting faster
    # With that, we can also allow for different NaN-orders by column
    # For this, we start with the last sort column
    # and use mergesort when we move to the front
    for col, asc, null_first in reversed(
        list(zip(sort_columns, sort_ascending, sort_null_first))
    ):
        if null_first:
            na_position = "first"
        else:
            na_position = "last"

        partition = partition.sort_values(
            by=[col], ascending=asc, na_position=na_position, kind="mergesort"
        )

    return partition
