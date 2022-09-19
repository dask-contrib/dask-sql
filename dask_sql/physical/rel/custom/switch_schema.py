from typing import TYPE_CHECKING

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan


class SwitchSchemaPlugin(BaseRelPlugin):
    """
    Show all MODELS currently registered/trained.
    The SQL is:

        SHOW MODELS

    The result is also a table, although it is created on the fly.
    """

    class_name = "UseSchema"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        use_schema = rel.use_schema()
        schema_name = str(use_schema.getSchemaName())
        if schema_name in context.schema:
            context.schema_name = schema_name
            # set the schema on the underlying DaskSQLContext as well
            context.context.use_schema(schema_name)
        else:
            raise RuntimeError(f"Schema {schema_name} not available")
