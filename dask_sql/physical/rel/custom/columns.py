from typing import TYPE_CHECKING

import dask.dataframe as dd
import pandas as pd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.mappings import python_to_sql_type
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_planner import LogicalPlan


class ShowColumnsPlugin(BaseRelPlugin):
    """
    Show all columns (and their types) for a given table.
    The SQL is:

        SHOW COLUMNS FROM <table>

    The result is also a table, although it is created on the fly.
    """

    class_name = "ShowColumns"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        schema_name = rel.show_columns().getSchemaName()
        name = rel.show_columns().getTableName()
        if not schema_name:
            schema_name = context.DEFAULT_SCHEMA_NAME

        dc = context.schema[schema_name].tables[name]

        cols = dc.column_container.columns
        dtypes = list(
            map(
                lambda x: str(python_to_sql_type(x).getSqlType())
                .rpartition(".")[2]
                .lower(),
                dc.df.dtypes,
            )
        )
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
