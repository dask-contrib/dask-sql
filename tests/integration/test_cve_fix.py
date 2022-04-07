import pytest

from dask_sql.server.app import _init_app


def test_run_server_disabled(c):
    with pytest.raises(NotImplementedError):
        c.run_server()


def test_init_app_disabled():
    with pytest.raises(NotImplementedError):
        _init_app()
