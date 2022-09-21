from typing import TYPE_CHECKING

import dask.dataframe as dd
import pandas as pd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_planner import LogicalPlan


class ShowSchemasPlugin(BaseRelPlugin):
    """
    Show all schemas.
    The SQL is:

        SHOW SCHEMAS

    The result is also a table, although it is created on the fly.
    """

    class_name = "ShowSchemas"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:

        show_schemas = rel.show_schemas()

        # "information_schema" is a schema which is found in every presto database
        schemas = list(context.schema.keys())
        schemas.append("information_schema")
        df = pd.DataFrame({"Schema": schemas})

        # We currently do not use the passed additional parameter FROM.
        like = str(show_schemas.getLike()).strip("'")
        if like and like != "None":
            df = df[df.Schema == like]

        cc = ColumnContainer(df.columns)
        dc = DataContainer(dd.from_pandas(df, npartitions=1), cc)
        return dc
