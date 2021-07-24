import logging

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

logger = logging.getLogger(__name__)


class CreateSchemaPlugin(BaseRelPlugin):
    """
    Create a table with given parameters from already existing data
    and register it at the context.
    The SQL call looks like

        CREATE TABLE <table-name> WITH (
            parameter = value,
            ...
        )

    It uses calls to "dask.dataframe.read_<format>"
    where format is given by the "format" parameter (defaults to CSV).
    The only mandatory parameter is the "location" parameter.

    Using this SQL is equivalent to just doing

        df = dd.read_<format>(location, **kwargs)
        context.register_dask_dataframe(df, <table-name>)

    but can also be used without writing a single line of code.
    Nothing is returned.
    """

    class_name = "com.dask.sql.parser.SqlCreateSchema"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ):
        schema_name = str(sql.getSchemaName())

        if schema_name in context.schema:
            if sql.getIfNotExists():
                return
            elif not sql.getReplace():
                raise RuntimeError(
                    f"A Schema with the name {schema_name} is already present."
                )

        context.create_schema(schema_name)
