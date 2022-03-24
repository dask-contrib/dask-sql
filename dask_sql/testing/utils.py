import os

from dask.dataframe.utils import assert_eq as _assert_eq


def assert_eq(*args, **kwargs):
    # use independent cluster for testing if it's available
    if os.getenv("DASK_SQL_TEST_SCHEDULER", None) is not None:
        kwargs.setdefault("scheduler", "distributed")
    return _assert_eq(*args, **kwargs)
