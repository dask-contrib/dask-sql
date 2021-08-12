import logging

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

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
