"""Integration between SQLAlchemy and Dask-SQL."""

from __future__ import absolute_import, unicode_literals

from sqlalchemy.engine import default

from . import dbapi


class DaskDialect(default.DefaultDialect):
    name = "dasksql"
    driver = "dask"
    supports_statement_cache = False

    @classmethod
    def dbapi(cls):
        return dbapi

    @classmethod
    def import_dbapi(cls):
        return dbapi

    def create_connect_args(self, url):
        return tuple(), dict()

    def do_rollback(self, dbapi_connection):
        # No transactions for Hive
        pass
