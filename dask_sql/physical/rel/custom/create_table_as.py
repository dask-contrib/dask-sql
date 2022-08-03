import logging
from collections import Counter
from typing import TYPE_CHECKING

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.rel.convert import RelConverter

if TYPE_CHECKING:
    import dask_sql
    from dask_planner import LogicalPlan

logger = logging.getLogger(__name__)


class CreateTableAsPlugin(BaseRelPlugin):
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

        # can we avoid hardcoding the schema name?
        schema_name, table_name = context.schema_name, create_memory_table.getName()

        if table_name in context.schema[schema_name].tables:
            if create_memory_table.getIfNotExists():
                return
            elif not create_memory_table.getOrReplace():
                raise RuntimeError(
                    f"A table with the name {table_name} is already present."
                )

        input_rel = create_memory_table.getInput()

        logger.debug(
            f"Creating new table with name {table_name} and logical plan {input_rel}"
        )

        dc = RelConverter.convert(input_rel, context=context)
        select_names = [field for field in input_rel.getRowType().getFieldList()]

        if select_names:
            # Use FQ name if not unique and simple name if it is unique. If a join contains the same column
            # names the output col is prepended with the fully qualified column name
            field_counts = Counter([field.getName() for field in select_names])
            select_names = [
                field.getQualifiedName()
                if field_counts[field.getName()] > 1
                else field.getName()
                for field in select_names
            ]

            cc = dc.column_container
            cc = cc.rename(
                {
                    df_col: select_name
                    for df_col, select_name in zip(cc.columns, select_names)
                }
            )
            dc = DataContainer(dc.df, cc)

        df = dc.assign()

        context.create_table(table_name, df, schema_name=schema_name)
