import logging
from typing import TYPE_CHECKING

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_sql._datafusion_lib import LogicalPlan

logger = logging.getLogger(__name__)


class CreateMemoryTablePlugin(BaseRelPlugin):
    """
    Create a table or view from the given SELECT query
    and register it at the context.
    The SQL call looks like

        CREATE TABLE <table-name> AS
            <some select query>

    It sends the select query through the normal parsing
    and optimization and conversation before registering it.

    Using this SQL is equivalent to just doing

        df = context.sql("<select query>")
        context.create_table(<table-name>, df)

    but can also be used without writing a single line of code.
    Nothing is returned.
    """

    class_name = ["CreateMemoryTable", "CreateView"]

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        # Rust create_memory_table instance handle
        create_memory_table = rel.create_memory_table()

        qualified_table_name = create_memory_table.getQualifiedName()
        *schema_name, table_name = qualified_table_name.split(".")

        if len(schema_name) > 1:
            raise RuntimeError(
                f"Expected unqualified or fully qualified table name, got {qualified_table_name}."
            )

        schema_name = context.schema_name if not schema_name else schema_name[0]

        if schema_name not in context.schema:
            raise RuntimeError(f"A schema with the name {schema_name} is not present.")
        if table_name in context.schema[schema_name].tables:
            if create_memory_table.getIfNotExists():
                return
            elif not create_memory_table.getOrReplace():
                raise RuntimeError(
                    f"A table with the name {table_name} is already present."
                )

        input_rel = create_memory_table.getInput()

        # TODO: we currently always persist for CREATE TABLE AS and never persist for CREATE VIEW AS;
        # should this be configured by the user? https://github.com/dask-contrib/dask-sql/issues/269
        persist = create_memory_table.isTable()

        logger.debug(
            f"Creating new table with name {qualified_table_name} and logical plan {input_rel}"
        )

        context.create_table(
            table_name,
            context._compute_table_from_rel(input_rel),
            persist=persist,
            schema_name=schema_name,
        )
