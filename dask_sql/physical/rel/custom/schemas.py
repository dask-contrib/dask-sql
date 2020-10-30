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

        SHOW SCHEMAS (FROM ... LIKE ...)

    The result is also a table, although it is created on the fly.
    """

    class_name = "com.dask.sql.parser.SqlShowSchemas"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        # "information_schema" is a schema which is found in every presto database
        schema = context.schema_name
        df = pd.DataFrame({"Schema": [schema, "information_schema"]})

        # We currently do not use the passed additional parameter FROM.
        like = str(sql.like).strip("'")
        if like and like != "None":
            df = df[df.Schema == like]

        cc = ColumnContainer(df.columns)
        dc = DataContainer(dd.from_pandas(df, npartitions=1), cc)
        return dc
