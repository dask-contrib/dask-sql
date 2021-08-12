import dask.dataframe as dd
import pandas as pd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin


class ShowTablesPlugin(BaseRelPlugin):
    """
    Show all tables currently defined for a given schema.
    The SQL is:

        SHOW TABLES FROM <schema>

    Please note that dask-sql currently
    only allows for a single schema (called "schema").

    The result is also a table, although it is created on the fly.
    """

    class_name = "com.dask.sql.parser.SqlShowTables"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        schema = str(sql.getSchema()).split(".")[-1]
        if schema not in context.schema:
            raise AttributeError(f"Schema {schema} is not defined.")

        df = pd.DataFrame({"Table": list(context.schema[schema].tables.keys())})

        cc = ColumnContainer(df.columns)
        dc = DataContainer(dd.from_pandas(df, npartitions=1), cc)
        return dc
