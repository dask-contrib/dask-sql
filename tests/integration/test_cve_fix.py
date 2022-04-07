import pytest

from dask_sql import Context
from dask_sql.server.app import _init_app, app


def test_run_server_disabled(c):
    with pytest.raises(NotImplementedError):
        c.run_server()


def test_init_app_disabled():
    c = Context()
    c.sql("SELECT 1 + 1").compute()
    with pytest.raises(NotImplementedError):
        _init_app(app, c)
