try:
    import fugue
    import fugue_dask
    from dask.distributed import Client
    from fugue import WorkflowDataFrame, register_execution_engine
    from fugue_sql import FugueSQLWorkflow
    from triad import run_at_def
    from triad.utils.convert import get_caller_global_local_vars
except ImportError:  # pragma: no cover
    raise ImportError(
        "Can not load the fugue module. If you want to use this integration, you need to install it."
    )

from typing import Any, Dict, Optional

import dask.dataframe as dd

from dask_sql.context import Context


@run_at_def
def _register_engines() -> None:
    """Register (overwrite) the default Dask execution engine of Fugue. This
    function is invoked as an entrypoint, users don't need to call it explicitly.
    """
    register_execution_engine(
        "dask",
        lambda conf, **kwargs: DaskSQLExecutionEngine(conf=conf),
        on_dup="overwrite",
    )

    register_execution_engine(
        Client,
        lambda engine, conf, **kwargs: DaskSQLExecutionEngine(
            dask_client=engine, conf=conf
        ),
        on_dup="overwrite",
    )


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

    @property
    def default_sql_engine(self) -> fugue.execution.execution_engine.SQLEngine:
        return self._default_sql_engine


def fsql_dask(
    sql: str,
    ctx: Optional[Context] = None,
    register: bool = False,
    fugue_conf: Any = None,
) -> Dict[str, dd.DataFrame]:
    """FugueSQL utility function that can consume Context directly. FugueSQL is a language
    extending standard SQL. It makes SQL eligible to describe end to end workflows. It also
    enables you to invoke python extensions in the SQL like language.

    For more, please read
    `FugueSQL Tutorial <https://fugue-tutorials.readthedocs.io/en/latest/tutorials/fugue_sql/index.html/>`_

    Args:
        sql (:obj:`str`): Fugue SQL statement
        ctx (:class:`dask_sql.Context`): The context to operate on, defaults to None
        register (:obj:`bool`): Whether to register named steps back to the context
          (if provided), defaults to False
        fugue_conf (:obj:`Any`): a dictionary like object containing Fugue specific configs

    Example:
        .. code-block:: python

            # define a custom prepartition function for FugueSQL
            def median(df: pd.DataFrame) -> pd.DataFrame:
                df["y"] = df["y"].median()
                return df.head(1)

            # create a context with some tables
            c = Context()
            ...

            # run a FugueSQL query using the context as input
            query = '''
                j = SELECT df1.*, df2.x
                    FROM df1 INNER JOIN df2 ON df1.key = df2.key
                    PERSIST
                TAKE 5 ROWS PREPARTITION BY x PRESORT key
                PRINT
                TRANSFORM j PREPARTITION BY x USING median
                PRINT
                '''
            result = fsql_dask(query, c, register=True)

            assert "j" in result
            assert "j" in c.tables
    """
    _global, _local = get_caller_global_local_vars()

    dag = FugueSQLWorkflow()
    dfs = (
        {}
        if ctx is None
        else {k: dag.df(v.df) for k, v in ctx.schema[ctx.schema_name].tables.items()}
    )
    result = dag._sql(sql, _global, _local, **dfs)
    dag.run(DaskSQLExecutionEngine(conf=fugue_conf))

    result_dfs = {
        k: v.result.native
        for k, v in result.items()
        if isinstance(v, WorkflowDataFrame)
    }
    if register and ctx is not None:
        for k, v in result_dfs.items():
            ctx.create_table(k, v)
    return result_dfs
