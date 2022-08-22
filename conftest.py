import pytest

pytest_plugins = ["tests.integration.fixtures"]


def pytest_addoption(parser):
    parser.addoption("--rungpu", action="store_true", help="run tests meant for GPU")
    parser.addoption("--runqueries", action="store_true", help="run test queries")


def pytest_runtest_setup(item):
    if "gpu" in item.keywords and not item.config.getoption("--rungpu"):
        pytest.skip("need --rungpu option to run")
    if "queries" in item.keywords and not item.config.getoption("--runqueries"):
        pytest.skip("need --runqueries option to run")
