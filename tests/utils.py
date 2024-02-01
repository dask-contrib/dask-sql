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


def convert_nullable_columns(df):
    """
    Convert certain nullable columns in `df` to non-nullable columns
    when trying to handle np.NaN and pd.NA would otherwise cause issues.
    """
    dtypes_mapping = {
        "Int64": "float64",
        "Float64": "float64",
        "boolean": "float64",
    }

    for dtype in dtypes_mapping:
        selected_cols = df.select_dtypes(include=[dtype]).columns.tolist()
        if selected_cols:
            df[selected_cols] = df[selected_cols].astype(dtypes_mapping[dtype])

    return df
