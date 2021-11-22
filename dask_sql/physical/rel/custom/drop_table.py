import logging
from typing import TYPE_CHECKING

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_sql.java import org

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
        schema_name, table_name = context.fqn(sql.getTableName())

        if table_name not in context.schema[schema_name].tables:
            if not sql.getIfExists():
                raise RuntimeError(
                    f"A table with the name {table_name} is not present."
                )
            else:
                return

        context.drop_table(table_name, schema_name=schema_name)
