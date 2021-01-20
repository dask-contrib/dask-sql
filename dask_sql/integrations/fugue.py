try:
    import fugue
    import fugue_dask
    from fugue_sql import FugueSQLWorkflow
    from triad.utils.convert import get_caller_global_local_vars
except ImportError:  # pragma: no cover
    raise ImportError(
        "Can not load the fugue module. If you want to use this integration, you need to install it."
    )

from dask_sql import Context
from typing import Any, Dict, Optional
import dask.dataframe as dd


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


def fsql(
    sql: str,
    ctx: Optional[Context] = None,
    register: bool = False,
    fugue_conf: Any = None,
) -> Dict[str, dd.DataFrame]:
    """Fugue SQL utility function that can consume Context directly. Fugue SQL is a language
    extending standard SQL. It makes SQL eligible to describe end to end workflows. It also
    enables you to invoke python extensions in the SQL like language.

    For more, please read
    `Fugue SQl Tutorial <https://fugue-tutorials.readthedocs.io/en/latest/tutorials/fugue_sql/index.html/>`_

    Args:
        sql: (:obj:`str`): Fugue SQL statement
        ctx (:class:`dask_sql.Context`): The context to operate on, defaults to None
        register (:obj:`bool`): Whether to register named steps back to the context
          (if provided), defaults to False
        fugue_conf (:obj:`Any`): a dictionary like object containing Fugue specific configs

    Example:
        .. code-block:: python
            # schema: *
            def median(df:pd.DataFrame) -> pd.DataFrame:
                df["y"] = df["y"].median()
                return df.head(1)

            # Create a context with tables df1, df2
            c = Context()
            ...
            result = fsql('''
            j = SELECT df1.*, df2.x
                FROM df1 INNER JOIN df2 ON df1.key = df2.key
                PERSIST  # using persist because j will be used twice
            TAKE 5 ROWS PREPARTITION BY x PRESORT key
            PRINT
            TRANSFORM j PREPARTITION BY x USING median
            PRINT
            ''', c, register=True)
            assert "j" in result
            assert "j" in c.tables

    """
    _global, _local = get_caller_global_local_vars()

    dag = FugueSQLWorkflow()
    dfs = {} if ctx is None else {k: dag.df(v.df) for k, v in ctx.tables.items()}
    result = dag._sql(sql, _global, _local, **dfs)
    dag.run(DaskSQLExecutionEngine(conf=fugue_conf))

    result_dfs = {k: v.result.native for k, v in result.items()}
    if register and ctx is not None:
        for k, v in result_dfs.items():
            ctx.create_table(k, v)
    return result_dfs
