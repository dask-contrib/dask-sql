import logging

from dask_sql.physical.rel.base import BaseRelPlugin

logger = logging.getLogger(__name__)


class AlterSchemaPlugin(BaseRelPlugin):
    """
    Alter schema name with new name;

       ALTER SCHEMA <old-schema-name> RENAME TO <new-schema-name>

    Using this SQL is equivalent to just doing

        context.alter_schema(<old-schema-name>,<new-schema-name>)

    but can also be used without writing a single line of code.
    Nothing is returned.
    """

    class_name = "com.dask.sql.parser.SqlAlterSchema"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ):
        old_schema_name = str(sql.getOldSchemaName())
        new_schema_name = str(sql.getNewSchemaName())

        print(old_schema_name, new_schema_name)
        if old_schema_name not in context.schema:
            raise RuntimeError(
                f"Schema {old_schema_name} was not found, Available schemas are - {context.schema.keys()}"
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

    class_name = "com.dask.sql.parser.SqlAlterTable"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ):
        old_table_name = str(sql.getOldTableName())
        new_table_name = str(sql.getNewTableName())

        if old_table_name not in context.schema[context.schema_name].tables:
            if not sql.getIfExists():
                raise RuntimeError(
                    f"TABLE {old_table_name} was not found, Available Tables in {context.schema_name} are "
                    f"- {context.schema[context.schema_name].tables.keys()}"
                )
            else:
                return

        context.alter_table(
            old_table_name=old_table_name, new_table_name=new_table_name
        )
