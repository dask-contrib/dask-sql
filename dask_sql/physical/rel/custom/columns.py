import pandas as pd
import dask.dataframe as dd

from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import DataContainer, ColumnContainer
from dask_sql.mappings import python_to_sql_type


class ShowColumnsPlugin(BaseRelPlugin):
    """
    Show all columns (and their types) for a given table.
    The SQL is:

        SHOW COLUMNS FROM <table>

    The result is also a table, although it is created on the fly.
    """

    class_name = "com.dask.sql.parser.SqlShowColumns"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        components = list(map(str, sql.getTable().names))

        # some queries might also include the database
        # as we do not have such a concept, we just get rid of it
        components = components[-2:]
        tableName = components[-1]

        if len(components) == 2:
            if components[0] != context.schema_name:
                raise AttributeError(f"Schema {components[0]} is not defined.")

        try:
            dc = context.tables[tableName]
        except KeyError:  # pragma: no cover
            raise AttributeError(f"Table {tableName} is not defined.")

        cols = dc.column_container.columns
        dtypes = list(map(lambda x: str(python_to_sql_type(x)).lower(), dc.df.dtypes))
        df = pd.DataFrame(
            {
                "Column": cols,
                "Type": dtypes,
                "Extra": [""] * len(cols),
                "Comment": [""] * len(cols),
            }
        )

        cc = ColumnContainer(df.columns)
        dc = DataContainer(dd.from_pandas(df, npartitions=1), cc)
        return dc
