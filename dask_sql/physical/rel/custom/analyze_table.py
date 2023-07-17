from typing import TYPE_CHECKING

import dask.dataframe as dd
import pandas as pd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.mappings import python_to_sql_type
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_sql._datafusion_lib import LogicalPlan


class AnalyzeTablePlugin(BaseRelPlugin):
    """
    Show information on the table (like mean, max etc.)
    on all or a subset of the columns..
    The SQL is:

        ANALYZE TABLE <table> COMPUTE STATISTICS FOR [ALL COLUMNS | COLUMNS a, b, ...]

    The result is also a table, although it is created on the fly.

    Please note: even though the syntax is very similar to e.g.
    [the spark version](https://spark.apache.org/docs/3.0.0/sql-ref-syntax-aux-analyze-table.html),
    this call does not help with query optimization (as the spark call would do),
    as this is currently not implemented in dask-sql.
    """

    class_name = "AnalyzeTable"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        analyze_table = rel.analyze_table()

        schema_name = analyze_table.getSchemaName() or context.schema_name
        table_name = analyze_table.getTableName()

        dc = context.schema[schema_name].tables[table_name]
        columns = analyze_table.getColumns()

        if not columns:
            columns = dc.column_container.columns

        # Define some useful shortcuts
        mapping = dc.column_container.get_backend_by_frontend_name
        df = dc.df

        # Calculate statistics
        statistics = dd.concat(
            [
                df[[mapping(col) for col in columns]].describe(),
                pd.DataFrame(
                    {
                        mapping(col): str(
                            python_to_sql_type(df[mapping(col)].dtype)
                        ).lower()
                        for col in columns
                    },
                    index=["data_type"],
                ),
                pd.DataFrame(
                    {mapping(col): col for col in columns}, index=["col_name"]
                ),
            ]
        )

        cc = ColumnContainer(statistics.columns)
        dc = DataContainer(statistics, cc)
        return dc
