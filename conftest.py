import pytest

pytest_plugins = ["tests.integration.fixtures", "distributed.utils_test"]


def pytest_addoption(parser):
    parser.addoption("--rungpu", action="store_true", help="run tests meant for GPU")


def pytest_runtest_setup(item):
    if "gpu" in item.keywords and not item.config.getoption("--rungpu"):
        pytest.skip("need --rungpu option to run")
