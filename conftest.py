import dask
import pytest

# determine if we can run CPU/GPU tests
try:
    import dask_cudf
except ImportError:
    dask_cudf = None

pytest_plugins = ["tests.integration.fixtures"]


def pytest_addoption(parser):
    parser.addoption("--rungpu", action="store_true", help="run tests meant for GPU")
    parser.addoption("--runqueries", action="store_true", help="run test queries")


def pytest_runtest_setup(item):
    if "queries" in item.keywords and not item.config.getoption("--runqueries"):
        pytest.skip("need --runqueries option to run")
    if "gpu" in item.keywords:
        if dask_cudf is None:
            pytest.skip("Test intended for CPU environments")
        dask.config.set({"dataframe.shuffle.algorithm": "tasks"})
    else:
        if "cpu" in item.keywords and dask_cudf is not None:
            pytest.skip("Test intended for CPU environments")
        dask.config.set({"dataframe.shuffle.algorithm": None})
