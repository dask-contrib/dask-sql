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
