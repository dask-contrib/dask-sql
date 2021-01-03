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
        #     +--------------+----------+
        # |     num_nulls|         0|
        # |distinct_count|         2|
        # |   avg_col_len|         4|
        # |   max_col_len|         4|
        # |     histogram|      NULL|

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
        statistics.append(df[[mapping(col) for col in columns]].describe())

        # Add additional information
        statistics.append(
            pd.Series(
                {
                    col: str(python_to_sql_type(df[mapping(col)].dtype)).lower()
                    for col in columns
                },
                name="data_type",
            )
        )
        statistics.append(pd.Series({col: col for col in columns}, name="col_name",))

        from dask.dataframe import methods
        from dask.utils import M
        from functools import reduce
        import numpy as np

        print(
            df.reduction(
                chunk=lambda chunk: pd.Series(
                    {mapping(col): chunk[mapping(col)].unique() for col in columns}
                ),
                combine=lambda x: {
                    mapping(col): reduce(
                        lambda x, y: np.hstack([x, y]), x[mapping(col)].values
                    )
                    for col in columns
                },
                aggregate=lambda x: pd.Series(
                    {mapping(col): len(x[mapping(col)]) for col in columns}
                ),
                split_every=None,
                # chunk_kwargs={"dropna": dropna},
                # aggregate_kwargs={"dropna": dropna},
            ).compute()
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
