from typing import TYPE_CHECKING

import dask.dataframe as dd
import pandas as pd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_planner import LogicalPlan


class ShowTablesPlugin(BaseRelPlugin):
    """
    Show all tables currently defined for a given schema.
    The SQL is:

        SHOW TABLES FROM <schema>

    Please note that dask-sql currently
    only allows for a single schema (called "schema").

    The result is also a table, although it is created on the fly.
    """

    class_name = "ShowTables"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        schema_name = rel.show_tables().getSchemaName() or context.schema_name

        if schema_name not in context.schema:
            raise AttributeError(f"Schema {schema_name} is not defined.")

        df = pd.DataFrame({"Table": list(context.schema[schema_name].tables.keys())})

        cc = ColumnContainer(df.columns)
        dc = DataContainer(dd.from_pandas(df, npartitions=1), cc)
        return dc
