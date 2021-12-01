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
    # when sort_values doesn't support lists of ascending / null
    # position booleans, we can still do the sort provided that
    # the list(s) are homogeneous:
    single_ascending = len(set(sort_ascending)) == 1
    single_null_first = len(set(sort_null_first)) == 1

    # pandas / cudf don't support lists of null positions
    if df.npartitions == 1 and single_null_first:
        return df.map_partitions(
            M.sort_values,
            by=sort_columns,
            ascending=sort_ascending,
            na_position="first" if sort_null_first[0] else "last",
        ).persist()

    # dask / dask-cudf don't support lists of ascending / null positions
    if len(sort_columns) == 1 or (
        dask_cudf is not None
        and isinstance(df, dask_cudf.DataFrame)
        and single_ascending
        and single_null_first
    ):
        try:
            return df.sort_values(
                by=sort_columns,
                ascending=sort_ascending[0],
                na_position="first" if sort_null_first[0] else "last",
                ignore_index=True,
            ).persist()
        except ValueError:
            pass

    # Dask doesn't natively support multi-column sorting;
    # we work around this by initially sorting by the first
    # column then handling the rest with `map_partitions`
    df = df.sort_values(
        by=sort_columns[0],
        ascending=sort_ascending[0],
        na_position="first" if sort_null_first[0] else "last",
    ).persist()

    # sort the remaining columns if given
    if len(sort_columns) > 1:
        df = df.map_partitions(
            make_pickable_without_dask_sql(sort_partition_func),
            meta=df,
            sort_columns=sort_columns,
            sort_ascending=sort_ascending,
            sort_null_first=sort_null_first,
        ).persist()

    return df


def sort_partition_func(
    partition: pd.DataFrame,
    sort_columns: List[str],
    sort_ascending: List[bool],
    sort_null_first: List[bool],
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
