import os

from dask.dataframe.utils import assert_eq as _assert_eq

# use independent cluster for testing if it's available
address = os.getenv("DASK_SQL_TEST_SCHEDULER", None)
scheduler = "sync" if address is None else "distributed"


def assert_eq(*args, **kwargs):
    kwargs.setdefault("scheduler", scheduler)

    return _assert_eq(*args, **kwargs)
