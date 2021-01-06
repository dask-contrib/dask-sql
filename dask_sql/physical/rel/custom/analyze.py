import pandas as pd
import dask.dataframe as dd

from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import DataContainer, ColumnContainer
from dask_sql.mappings import python_to_sql_type
from dask_sql.utils import get_table_from_compound_identifier


class AnalyzeTablePlugin(BaseRelPlugin):
    """
    Show information on the table (like mean, max etc.)
    on all or a subset of the columns..
    The SQL is:

        ANALYZE TABLE <table> COMPUTE STATISTICS [FOR ALL COLUMNS | FOR COLUMNS a, b, ...]

    The result is also a table, although it is created on the fly.

    Please note: even though the syntax is very similar to e.g.
    [the spark version](https://spark.apache.org/docs/3.0.0/sql-ref-syntax-aux-analyze-table.html),
    this call does not help with query optimization (as the spark call would do),
    as this is currently not implemented in dask-sql.
    """

    class_name = "com.dask.sql.parser.SqlAnalyzeTable"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        components = list(map(str, sql.getTableName().names))
        dc = get_table_from_compound_identifier(context, components)
        columns = list(map(str, sql.getColumnList()))

        if not columns:
            columns = dc.column_container.columns

        # Define some useful shortcuts
        mapping = dc.column_container.get_backend_by_frontend_name
        df = dc.df

        # Calculate statistics
        statistics = dd.from_pandas(
            pd.DataFrame({col: [] for col in columns}), npartitions=1
        )
        statistics = statistics.append(df[[mapping(col) for col in columns]].describe())

        # Add additional information
        statistics = statistics.append(
            pd.Series(
                {
                    col: str(python_to_sql_type(df[mapping(col)].dtype)).lower()
                    for col in columns
                },
                name="data_type",
            )
        )
        statistics = statistics.append(
            pd.Series({col: col for col in columns}, name="col_name",)
        )

        cc = ColumnContainer(statistics.columns)
        dc = DataContainer(statistics, cc)
        return dc
