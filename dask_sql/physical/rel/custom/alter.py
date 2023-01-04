import logging
from typing import TYPE_CHECKING

from dask_sql.physical.rel.base import BaseRelPlugin

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan


class AlterSchemaPlugin(BaseRelPlugin):
    """
    Alter schema name with new name;

       ALTER SCHEMA <old-schema-name> RENAME TO <new-schema-name>

    Using this SQL is equivalent to just doing

        context.alter_schema(<old-schema-name>,<new-schema-name>)

    but can also be used without writing a single line of code.
    Nothing is returned.
    """

    class_name = "AlterSchema"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context"):
        alter_schema = rel.alter_schema()

        old_schema_name = alter_schema.getOldSchemaName()
        new_schema_name = alter_schema.getNewSchemaName()

        logger.info(
            f"changing schema name from `{old_schema_name}` to `{new_schema_name}`"
        )
        if old_schema_name not in context.schema:
            raise KeyError(
                f"Schema {old_schema_name} was not found, available schemas are - {context.schema.keys()}"
            )
        context.alter_schema(
            old_schema_name=old_schema_name, new_schema_name=new_schema_name
        )


class AlterTablePlugin(BaseRelPlugin):
    """
    Alter table name with new name;

       ALTER TABLE [IF EXISTS] <old-table-name> RENAME TO <new-table-name>

    Using this SQL is equivalent to just doing

        context.alter_table(<old-table-name>,<new-table-name>)

    but can also be used without writing a single line of code.
    Nothing is returned.
    """

    class_name = "AlterTable"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context"):
        alter_table = rel.alter_table()

        old_table_name = alter_table.getOldTableName()
        new_table_name = alter_table.getNewTableName()
        schema_name = alter_table.getSchemaName() or context.schema_name

        logger.info(
            f"changing table name from `{old_table_name}` to `{new_table_name}`"
        )
        if old_table_name not in context.schema[schema_name].tables:
            if not alter_table.getIfExists():
                raise KeyError(
                    f"Table {old_table_name} was not found, available tables in {schema_name} are "
                    f"- {context.schema[schema_name].tables.keys()}"
                )
            else:
                return

        context.alter_table(
            old_table_name=old_table_name,
            new_table_name=new_table_name,
            schema_name=schema_name,
        )
