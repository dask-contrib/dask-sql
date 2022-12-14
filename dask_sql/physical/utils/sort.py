from typing import List

import dask.dataframe as dd
import pandas as pd
from dask import config as dask_config
from dask.utils import M

from dask_sql.utils import make_pickable_without_dask_sql


def apply_sort(
    df: dd.DataFrame,
    sort_columns: List[str],
    sort_ascending: List[bool],
    sort_null_first: List[bool],
    sort_num_rows: int = None,
) -> dd.DataFrame:
    # when sort_values doesn't support lists of ascending / null
    # position booleans, we can still do the sort provided that
    # the list(s) are homogeneous:
    single_ascending = len(set(sort_ascending)) == 1
    single_null_first = len(set(sort_null_first)) == 1

    if is_topk_optimizable(
        df=df,
        sort_columns=sort_columns,
        single_ascending=single_ascending,
        sort_null_first=sort_null_first,
        sort_num_rows=sort_num_rows,
    ):
        return topk_sort(
            df=df,
            sort_columns=sort_columns,
            sort_ascending=sort_ascending,
            sort_num_rows=sort_num_rows,
        )

    else:
        # Pre persist before sort to avoid duplicate compute
        df = df.persist()

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
        "cudf" in str(df._partition_type) and single_ascending and single_null_first
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


def topk_sort(
    df: dd.DataFrame,
    sort_columns: List[str],
    sort_ascending: List[bool],
    sort_num_rows: int = None,
):
    if sort_ascending[0]:
        return df.nsmallest(n=sort_num_rows, columns=sort_columns)
    else:
        return df.nlargest(n=sort_num_rows, columns=sort_columns)


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


def is_topk_optimizable(
    df: dd.DataFrame,
    sort_columns: List[str],
    single_ascending: bool,
    sort_null_first: List[bool],
    sort_num_rows: int = None,
):
    if (
        sort_num_rows is None
        or not single_ascending
        or any(sort_null_first)
        # pandas doesnt support nsmallest/nlargest with object dtypes
        or (
            "pandas" in str(df._partition_type)
            and any(df[sort_columns].dtypes == "object")
        )
        or (
            sort_num_rows * len(df.columns)
            > dask_config.get("sql.sort.topk-nelem-limit")
        )
    ):
        return False

    return True
