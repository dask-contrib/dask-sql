import logging
from typing import TYPE_CHECKING

from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_sql.java import org

logger = logging.getLogger(__name__)


class CreateSchemaPlugin(BaseRelPlugin):
    """
    Create a schema with the given name
    and register it at the context.
    The SQL call looks like

        CREATE SCHEMA <schema-name>

    Using this SQL is equivalent to just doing

        context.create_schema(<schema-name>)

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
