import logging

from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import DataContainer
from dask_sql.mappings import sql_to_python_value

logger = logging.getLogger(__name__)


class CreateAsPlugin(BaseRelPlugin):
    """
    Create a table or view from the given SELECT query
    and register it at the context.
    The SQL call looks like

        CREATE TABLE <table-name> AS
            <some select query>

    It sends the select query through the normal parsing
    and optimization and conversation before registering it.

    Using this SQL is equivalent to just doing

        df = context.sql("<select query>")
        context.create_table(<table-name>, df)

    but can also be used without writing a single line of code.
    Nothing is returned.
    """

    class_name = "com.dask.sql.parser.SqlCreateTableAs"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        sql_select = sql.getSelect()
        table_name = str(sql.getTableName())
        persist = bool(sql.isPersist())

        logger.debug(
            f"Creating new table with name {table_name} and query {sql_select}"
        )

        sql_select_query = context._to_sql_string(sql_select)
        df = context.sql(sql_select_query)

        context.create_table(table_name, df, persist=persist)
