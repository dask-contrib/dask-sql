from typing import List

import dask.dataframe as dd

from dask_sql.utils import new_temporary_column


def get_groupby_with_nulls_cols(
    df: dd.DataFrame, group_columns: List[str], additional_column_name: str = None
):
    """
    SQL and dask are treating null columns a bit different:
    SQL will put them to the front, dask will just ignore them
    Therefore we use the same trick as fugue does:
    we will group by both the NaN and the real column value
    """
    if additional_column_name is None:
        additional_column_name = new_temporary_column(df)

    group_columns_and_nulls = []
    for group_column in group_columns:
        is_null_column = group_column.isnull()
        non_nan_group_column = group_column.fillna(0)

        # split_out doesn't work if both columns have the same name
        is_null_column.name = f"{is_null_column.name}_{new_temporary_column(df)}"

        group_columns_and_nulls += [is_null_column, non_nan_group_column]

    if not group_columns_and_nulls:
        # This can happen in statements like
        # SELECT SUM(x) FROM data
        # without any groupby statement
        group_columns_and_nulls = [additional_column_name]

    return group_columns_and_nulls
