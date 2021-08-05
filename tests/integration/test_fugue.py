import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

fugue_sql = pytest.importorskip("fugue_sql")
import dask.dataframe as dd

from dask_sql import Context

# needs to be imported after the check for fugue
from dask_sql.integrations.fugue import DaskSQLExecutionEngine, fsql_dask


def test_simple_statement():
    dag = fugue_sql.FugueSQLWorkflow()
    df = dag.df([[0, "hello"], [1, "world"]], "a:int64,b:str")
    dag(
        """
    SELECT * FROM df WHERE a > 0 YIELD DATAFRAME AS result
    """
    )
    result = dag.run(DaskSQLExecutionEngine)

    return_df = result["result"].as_pandas()
    assert_frame_equal(return_df, pd.DataFrame({"a": [1], "b": ["world"]}))

    # A more elegant way to do things
    pdf = pd.DataFrame([[0, "hello"], [1, "world"]], columns=["a", "b"])
    result = fugue_sql.fsql(
        """
    SELECT * FROM df WHERE a > 0 YIELD DATAFRAME AS result
    """,
        df=pdf,
    ).run("dask")

    return_df = result["result"].as_pandas()
    assert_frame_equal(return_df, pd.DataFrame({"a": [1], "b": ["world"]}))


def test_fsql():
    def assert_eq(df: pd.DataFrame) -> None:
        assert_frame_equal(df, pd.DataFrame({"a": [1]}))

    # the simplest case: the SQL does not use any input and does not generate output
    fsql_dask(
        """
    CREATE [[0],[1]] SCHEMA a:long
    SELECT * WHERE a>0
    OUTPUT USING assert_eq
    """
    )

    # it can directly use the dataframes inside dask-sql Context
    c = Context()
    c.create_table(
        "df", dd.from_pandas(pd.DataFrame([[0], [1]], columns=["a"]), npartitions=2)
    )

    fsql_dask(
        """
    SELECT * FROM df WHERE a>0
    OUTPUT USING assert_eq
    """,
        c,
    )

    # for dataframes with name, they can register back to the Context (register=True)
    # the return of fsql is the dict of all dask dataframes with explicit names
    result = fsql_dask(
        """
    x=SELECT * FROM df WHERE a>0
    OUTPUT USING assert_eq
    """,
        c,
        register=True,
    )
    assert isinstance(result["x"], dd.DataFrame)
    assert "x" in c.schema[c.schema_name].tables

    # integration test with fugue transformer extension
    c = Context()
    c.create_table(
        "df1",
        dd.from_pandas(
            pd.DataFrame([[0, 1], [1, 2]], columns=["a", "b"]), npartitions=2
        ),
    )
    c.create_table(
        "df2",
        dd.from_pandas(
            pd.DataFrame([[1, 2], [3, 4], [-4, 5]], columns=["a", "b"]), npartitions=2
        ),
    )

    # schema: *
    def cumsum(df: pd.DataFrame) -> pd.DataFrame:
        return df.cumsum()

    fsql_dask(
        """
    data = SELECT * FROM df1 WHERE a>0 UNION ALL SELECT * FROM df2 WHERE a>0 PERSIST
    result1 = TRANSFORM data PREPARTITION BY a PRESORT b USING cumsum
    result2 = TRANSFORM data PREPARTITION BY b PRESORT a USING cumsum
    PRINT result1, result2
    """,
        c,
        register=True,
    )
    assert "result1" in c.schema[c.schema_name].tables
    assert "result2" in c.schema[c.schema_name].tables
