import os
import tempfile

import pytest
import dask.dataframe as dd
import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np


@pytest.fixture()
def df_simple():
    return pd.DataFrame({"a": [1, 2, 3], "b": [1.1, 2.2, 3.3]})


@pytest.fixture()
def df():
    np.random.seed(42)
    return pd.DataFrame(
        {"a": [1.0] * 100 + [2.0] * 200 + [3.0] * 400, "b": 10 * np.random.rand(700),}
    )


@pytest.fixture()
def user_table_1():
    return pd.DataFrame({"user_id": [2, 1, 2, 3], "b": [3, 3, 1, 3]})


@pytest.fixture()
def user_table_2():
    return pd.DataFrame({"user_id": [1, 1, 2, 4], "c": [1, 2, 3, 4]})


@pytest.fixture()
def long_table():
    return pd.DataFrame({"a": [0] * 100 + [1] * 101 + [2] * 103})


@pytest.fixture()
def user_table_inf():
    return pd.DataFrame({"c": [3, float("inf"), 1]})


@pytest.fixture()
def user_table_nan():
    return pd.DataFrame({"c": [3, pd.NA, 1]})


@pytest.fixture()
def string_table():
    return pd.DataFrame({"a": ["a normal string", "%_%", "^|()-*[]$"]})


@pytest.fixture()
def datetime_table():
    return pd.DataFrame(
        {
            "timezone": pd.date_range(
                start="2014-08-01 09:00", freq="H", periods=3, tz="Europe/Berlin"
            ),
            "no_timezone": pd.date_range(start="2014-08-01 09:00", freq="H", periods=3),
            "utc_timezone": pd.date_range(
                start="2014-08-01 09:00", freq="H", periods=3, tz="UTC"
            ),
        }
    )


@pytest.fixture()
def c(
    df_simple,
    df,
    user_table_1,
    user_table_2,
    long_table,
    user_table_inf,
    user_table_nan,
    string_table,
    datetime_table,
):
    dfs = {
        "df_simple": df_simple,
        "df": df,
        "user_table_1": user_table_1,
        "user_table_2": user_table_2,
        "long_table": long_table,
        "user_table_inf": user_table_inf,
        "user_table_nan": user_table_nan,
        "string_table": string_table,
        "datetime_table": datetime_table,
    }

    # Lazy import, otherwise the pytest framework has problems
    from dask_sql.context import Context

    c = Context()
    for df_name, df in dfs.items():
        dask_df = dd.from_pandas(df, npartitions=3)
        c.create_table(df_name, dask_df)

    yield c


@pytest.fixture()
def temporary_data_file():
    temporary_data_file = os.path.join(
        tempfile.gettempdir(), os.urandom(24).hex() + ".csv"
    )

    yield temporary_data_file

    if os.path.exists(temporary_data_file):
        os.unlink(temporary_data_file)


@pytest.fixture()
def assert_query_gives_same_result(engine):
    np.random.seed(42)

    df1 = dd.from_pandas(
        pd.DataFrame(
            {
                "user_id": np.random.choice([1, 2, 3, 4, pd.NA], 100),
                "a": np.random.rand(100),
                "b": np.random.randint(-10, 10, 100),
            }
        ),
        npartitions=3,
    )
    df1["user_id"] = df1["user_id"].astype("Int64")

    df2 = dd.from_pandas(
        pd.DataFrame(
            {
                "user_id": np.random.choice([1, 2, 3, 4], 100),
                "c": np.random.randint(20, 30, 100),
                "d": np.random.choice(["a", "b", "c", None], 100),
            }
        ),
        npartitions=3,
    )

    df3 = dd.from_pandas(
        pd.DataFrame(
            {
                "s": [
                    "".join(np.random.choice(["a", "B", "c", "D"], 10))
                    for _ in range(100)
                ]
                + [None]
            }
        ),
        npartitions=3,
    )

    # the other is a Int64, that makes joining simpler
    df2["user_id"] = df2["user_id"].astype("Int64")

    # add some NaNs
    df1["a"] = df1["a"].apply(
        lambda a: float("nan") if a > 0.8 else a, meta=("a", "float")
    )
    df1["b_bool"] = df1["b"].apply(
        lambda b: pd.NA if b > 5 else b < 0, meta=("a", "bool")
    )

    # Lazy import, otherwise the pytest framework has problems
    from dask_sql.context import Context

    c = Context()
    c.create_table("df1", df1)
    c.create_table("df2", df2)
    c.create_table("df3", df3)

    df1.compute().to_sql("df1", engine, index=False, if_exists="replace")
    df2.compute().to_sql("df2", engine, index=False, if_exists="replace")
    df3.compute().to_sql("df3", engine, index=False, if_exists="replace")

    def _assert_query_gives_same_result(query, sort_columns=None, **kwargs):
        sql_result = pd.read_sql_query(query, engine)
        dask_result = c.sql(query).compute()

        # allow that the names are different
        # as expressions are handled differently
        dask_result.columns = sql_result.columns

        if sort_columns:
            sql_result = sql_result.sort_values(sort_columns)
            dask_result = dask_result.sort_values(sort_columns)

        sql_result = sql_result.reset_index(drop=True)
        dask_result = dask_result.reset_index(drop=True)

        assert_frame_equal(sql_result, dask_result, check_dtype=False, **kwargs)

    return _assert_query_gives_same_result
