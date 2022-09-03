import logging
from typing import TYPE_CHECKING

from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan

logger = logging.getLogger(__name__)


class DropSchemaPlugin(BaseRelPlugin):
    """
    Drop a schema with given name.
    The SQL call looks like

        DROP SCHEMA <schema-name>
    """

    class_name = "DropSchema"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context"):
        drop_schema = rel.drop_schema()
        schema_name = str(drop_schema.getSchemaName())

        if schema_name not in context.schema:
            if not drop_schema.getIfExists():
                raise RuntimeError(
                    f"A SCHEMA with the name {schema_name} is not present."
                )
            else:
                return

        context.drop_schema(schema_name)
