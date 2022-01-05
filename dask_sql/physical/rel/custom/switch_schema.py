from typing import TYPE_CHECKING

from nvtx import annotate

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_sql.java import org


class SwitchSchemaPlugin(BaseRelPlugin):
    """
    Show all MODELS currently registered/trained.
    The SQL is:

        SHOW MODELS

    The result is also a table, although it is created on the fly.
    """

    class_name = "com.dask.sql.parser.SqlUseSchema"

    @annotate("SWITCH_SCHEMA_PLUGIN_CONVERT", color="green", domain="dask_sql_python")
    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        schema_name = str(sql.getSchemaName())
        if schema_name in context.schema:
            context.schema_name = schema_name
        else:
            raise RuntimeError(f"Schema {schema_name} not available")
