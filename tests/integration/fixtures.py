import os
import tempfile

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest
from dask.datasets import timeseries as dd_timeseries
from dask.distributed import Client

from tests.utils import assert_eq

try:
    from dask_cuda import LocalCUDACluster
except ImportError:
    LocalCUDACluster = None

# check if we want to connect to an independent cluster
SCHEDULER_ADDR = os.getenv("DASK_SQL_TEST_SCHEDULER", None)


@pytest.fixture(params=[False, pytest.param(True, marks=pytest.mark.gpu)])
def c(request, tmpdir):
    # Lazy import, otherwise the pytest framework has problems
    from dask_sql._compat import INT_NAN_IMPLEMENTED
    from dask_sql.context import Context

    np.random.seed(42)

    dfs = {
        "df_simple": pd.DataFrame({"a": [1, 2, 3], "b": [1.1, 2.2, 3.3]}),
        "df_wide": pd.DataFrame(
            {
                "a": [0, 1, 2],
                "b": [3, 4, 5],
                "c": [6, 7, 8],
                "d": [9, 10, 11],
                "e": [12, 13, 14],
            }
        ),
        "df": pd.DataFrame(
            {
                "a": [1.0] * 100 + [2.0] * 200 + [3.0] * 400,
                "b": 10 * np.random.rand(700),
            }
        ),
        "department_table": pd.DataFrame(
            {"department_name": ["English", "Math", "Science"]}
        ),
        "user_table_1": pd.DataFrame({"user_id": [2, 1, 2, 3], "b": [3, 3, 1, 3]}),
        "user_table_2": pd.DataFrame({"user_id": [1, 1, 2, 4], "c": [1, 2, 3, 4]}),
        "long_table": pd.DataFrame({"a": [0] * 100 + [1] * 101 + [2] * 103}),
        "user_table_inf": pd.DataFrame({"c": [3, float("inf"), 1]}),
        "user_table_nan": pd.DataFrame({"c": [3, pd.NA, 1]}).astype("UInt8")
        if INT_NAN_IMPLEMENTED
        else pd.DataFrame({"c": [3, float("nan"), 1]}).astype("float"),
        "string_table": pd.DataFrame(
            {
                "a": [
                    "a normal string",
                    "%_%",
                    "^|()-*[]$",
                    "^|()-*[]$\n%_%\na normal string",
                ]
            }
        ),
        "datetime_table": pd.DataFrame(
            {
                "timezone": pd.date_range(
                    start="2014-08-01 09:00", freq="8H", periods=6, tz="Europe/Berlin"
                ),
                "no_timezone": pd.date_range(
                    start="2014-08-01 09:00", freq="8H", periods=6
                ),
                "utc_timezone": pd.date_range(
                    start="2014-08-01 09:00", freq="8H", periods=6, tz="UTC"
                ),
            }
        ),
        "timeseries": dd_timeseries(freq="1d").reset_index(drop=True),
    }

    # Write simple parquet dataset
    df = pd.DataFrame(
        {
            "a": [1, 2, 3] * 5,
            "b": range(15),
            "c": ["A"] * 15,
            "d": [
                pd.Timestamp("2013-08-01 23:00:00"),
                pd.Timestamp("2014-09-01 23:00:00"),
                pd.Timestamp("2015-10-01 23:00:00"),
            ]
            * 5,
            "index": range(15),
        },
    )
    dd.from_pandas(df, npartitions=3).to_parquet(os.path.join(tmpdir, "parquet"))

    gpu = request.param

    c = Context(gpu=gpu)
    for df_name, df in dfs.items():
        if gpu and df_name == "datetime_table":
            # cudf doesn't have support for timezoned datetime data
            df["timezone"] = df["timezone"].astype("datetime64[ns]")
            df["utc_timezone"] = df["utc_timezone"].astype("datetime64[ns]")
        c.create_table(df_name, df)

    # read back in parquet dataset
    c.create_table("parquet_ddf", str(tmpdir), format="parquet")

    yield c


@pytest.fixture()
def df_simple(c):
    return c.sql("select * from df_simple").compute()


@pytest.fixture()
def df_wide(c):
    return c.sql("select * from df_wide").compute()


@pytest.fixture()
def df(c):
    return c.sql("select * from df").compute()


@pytest.fixture()
def department_table(c):
    return c.sql("select * from department_table").compute()


@pytest.fixture()
def user_table_1(c):
    return c.sql("select * from user_table_1").compute()


@pytest.fixture()
def user_table_2(c):
    return c.sql("select * from user_table_2").compute()


@pytest.fixture()
def long_table(c):
    return c.sql("select * from long_table").compute()


@pytest.fixture()
def user_table_inf(c):
    return c.sql("select * from user_table_inf").compute()


@pytest.fixture()
def user_table_nan(c):
    return c.sql("select * from user_table_nan").compute()


@pytest.fixture()
def string_table(c):
    return c.sql("select * from string_table").compute()


@pytest.fixture()
def datetime_table(c):
    return c.sql("select * from datetime_table").compute()


@pytest.fixture()
def timeseries(c):
    return c.sql("select * from timeseries").compute()


@pytest.fixture()
def parquet_ddf(c):
    return c.sql("select * from parquet_ddf")


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

        assert_eq(sql_result, dask_result, check_dtype=False, **kwargs)

    return _assert_query_gives_same_result


@pytest.fixture()
def gpu_client(request):
    # allow gpu_client to be used directly as a fixture or parametrized
    if not hasattr(request, "param") or request.param:
        with LocalCUDACluster(protocol="tcp") as cluster:
            with Client(cluster) as client:
                yield client
    else:
        with Client(address=SCHEDULER_ADDR) as client:
            yield client


# if connecting to an independent cluster, use a session-wide
# client for all computations. otherwise, only connect to a client
# when specified.
@pytest.fixture(
    scope="function",
    autouse=False if SCHEDULER_ADDR is None else True,
)
def client():
    with Client(address=SCHEDULER_ADDR) as client:
        yield client


xfail_if_external_scheduler = pytest.mark.xfail(
    condition=os.getenv("DASK_SQL_TEST_SCHEDULER", None) is not None,
    reason="Can not run with external cluster",
)
