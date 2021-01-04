import logging

from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import DataContainer

logger = logging.getLogger(__name__)


class DropTablePlugin(BaseRelPlugin):
    """
    Drop a table with given name.
    The SQL call looks like

        DROP TABLE <table-name>
    """

    class_name = "com.dask.sql.parser.SqlDropTable"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        table_name = str(sql.getTableName())

        if table_name not in context.tables:
            if not sql.getIfExists():
                raise RuntimeError(
                    f"A table with the name {table_name} is not present."
                )
            else:
                return

        del context.tables[table_name]
