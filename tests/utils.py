import os

from dask.dataframe.utils import assert_eq as _assert_eq

# use distributed client for testing if it's available
scheduler = (
    "distributed"
    if os.getenv("DASK_SQL_DISTRIBUTED_TESTS", "False").lower() in ("true", "1")
    else "sync"
)


def assert_eq(*args, **kwargs):
    kwargs.setdefault("scheduler", scheduler)

    return _assert_eq(*args, **kwargs)


def normalize_dask_result(df):
    datetime_cols = df.select_dtypes(include=["datetime64[ns]"]).columns.tolist()
    nullable_cols = df.select_dtypes(include=["Int64", "Float64", "string[python]"]).columns.tolist()

    if not datetime_cols and not nullable_cols:
        return df

    # casting to object to ensure equality with sql-lite
    # which returns object dtype for datetime inputs
    for col in datetime_cols:
        df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    # converting nullable dtype columns that cannot be filled with np.nan 
    for col in nullable_cols:
        if df[col].dtype in ("Int64", "Float64"):
            df[col] = df[col].astype("float64")
        elif df[col].dtype == "string[python]":
            df[col] = df[col].astype("object")

    return df
