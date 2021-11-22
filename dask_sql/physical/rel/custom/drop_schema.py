import logging
from typing import TYPE_CHECKING

from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_sql.java import org

logger = logging.getLogger(__name__)


class DropSchemaPlugin(BaseRelPlugin):
    """
    Drop a schema with given name.
    The SQL call looks like

        DROP SCHEMA <schema-name>
    """

    class_name = "com.dask.sql.parser.SqlDropSchema"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ):
        schema_name = str(sql.getSchemaName())

        if schema_name not in context.schema:
            if not sql.getIfExists():
                raise RuntimeError(
                    f"A SCHEMA with the name {schema_name} is not present."
                )
            else:
                return

        context.drop_schema(schema_name)
