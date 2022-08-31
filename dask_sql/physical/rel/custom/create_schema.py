import logging
from typing import TYPE_CHECKING

from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan

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

    class_name = "CreateCatalogSchema"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context"):
        create_schema = rel.create_catalog_schema()
        schema_name = str(create_schema.getSchemaName())

        if schema_name in context.schema:
            if create_schema.getIfNotExists():
                return
            elif not create_schema.getReplace():
                raise RuntimeError(
                    f"A Schema with the name {schema_name} is already present."
                )

        context.create_schema(schema_name)
