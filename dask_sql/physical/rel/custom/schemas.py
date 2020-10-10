import pandas as pd
import dask.dataframe as dd

from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import DataContainer, ColumnContainer


class ShowSchemasPlugin(BaseRelPlugin):
    """
    Show all schemas. Please note that dask-sql currently
    only allows for a single schema (called "schema"),
    but some external applications need to have this
    functionality.
    The SQL is:

        SHOW SCHEMAS

    The result is also a table, although it is created on the fly.
    """

    class_name = "com.dask.sql.parser.SqlShowSchemas"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        df = pd.DataFrame({"Schema": [context.schema_name]})

        cc = ColumnContainer(df.columns)
        dc = DataContainer(dd.from_pandas(df, npartitions=1), cc)
        return dc
