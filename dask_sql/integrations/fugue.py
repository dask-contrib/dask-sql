try:
    import fugue
    import fugue_dask
except ImportError:  # pragma: no cover
    raise ImportError(
        "Can not load the fugue module. If you want to use this integration, you need to install it."
    )

from dask_sql import Context


class DaskSQLEngine(fugue.execution.execution_engine.SQLEngine):
    """
    SQL engine for fugue which uses dask-sql instead of the native
    SQL implementation.

    Please note, that so far the native SQL engine in fugue
    understands a larger set of SQL commands, but in turns is
    (on average) slower in computation and scaling.
    """

    def __init__(self, *args, **kwargs):
        """Create a new instance."""
        super().__init__(*args, **kwargs)

    def select(
        self, dfs: fugue.dataframe.DataFrames, statement: str
    ) -> fugue.dataframe.DataFrame:
        """Send the SQL command to the dask-sql context and register all temporary dataframes"""
        c = Context()

        for k, v in dfs.items():
            c.create_table(k, self.execution_engine.to_df(v).native)

        df = c.sql(statement)
        return fugue_dask.dataframe.DaskDataFrame(df)


class DaskSQLExecutionEngine(fugue_dask.DaskExecutionEngine):
    """
    Execution engine for fugue which has dask-sql as SQL engine
    configured.

    Please note, that so far the native SQL engine in fugue
    understands a larger set of SQL commands, but in turns is
    (on average) slower in computation and scaling.
    """

    def __init__(self, *args, **kwargs):
        """Create a new instance."""
        super().__init__(*args, **kwargs)
        self._default_sql_engine = DaskSQLEngine(self)
