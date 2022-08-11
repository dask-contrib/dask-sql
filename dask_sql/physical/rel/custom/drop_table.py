import logging
from typing import TYPE_CHECKING

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_sql.rust import LogicalPlan

logger = logging.getLogger(__name__)


class DropTablePlugin(BaseRelPlugin):
    """
    Drop a table with given name.
    The SQL call looks like

        DROP TABLE <table-name>
    """

    class_name = "DropTable"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        # Rust create_memory_table instance handle
        drop_table = rel.drop_table()

        # can we avoid hardcoding the schema name?
        schema_name, table_name = context.schema_name, drop_table.getName()

        if table_name not in context.schema[schema_name].tables:
            if not drop_table.getIfExists():
                raise RuntimeError(
                    f"A table with the name {table_name} is not present."
                )
            else:
                return

        context.drop_table(table_name, schema_name=schema_name)
