from typing import TYPE_CHECKING

import dask.dataframe as dd
import pandas as pd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_sql.java import org


class ShowSchemasPlugin(BaseRelPlugin):
    """
    Show all schemas.
    The SQL is:

        SHOW SCHEMAS (FROM ... LIKE ...)

    The result is also a table, although it is created on the fly.
    """

    class_name = "com.dask.sql.parser.SqlShowSchemas"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        # "information_schema" is a schema which is found in every presto database
        schemas = list(context.schema.keys())
        schemas.append("information_schema")
        df = pd.DataFrame({"Schema": schemas})

        # We currently do not use the passed additional parameter FROM.
        like = str(sql.like).strip("'")
        if like and like != "None":
            df = df[df.Schema == like]

        cc = ColumnContainer(df.columns)
        dc = DataContainer(dd.from_pandas(df, npartitions=1), cc)
        return dc
