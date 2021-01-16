import logging

from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import DataContainer
from dask_sql.utils import convert_sql_kwargs

logger = logging.getLogger(__name__)


class CreateTablePlugin(BaseRelPlugin):
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

    class_name = "com.dask.sql.parser.SqlCreateTable"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        table_name = str(sql.getTableName())

        if table_name in context.tables:
            if sql.getIfNotExists():
                return
            elif not sql.getReplace():
                raise RuntimeError(
                    f"A table with the name {table_name} is already present."
                )

        kwargs = convert_sql_kwargs(sql.getKwargs())

        logger.debug(
            f"Creating new table with name {table_name} and parameters {kwargs}"
        )

        format = kwargs.pop("format", "csv").lower()
        persist = kwargs.pop("persist", False)

        try:
            location = kwargs.pop("location")
        except KeyError:
            raise AttributeError("Parameters must include a 'location' parameter.")

        context.create_table(
            table_name, location, format=format, persist=persist, **kwargs
        )
