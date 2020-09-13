import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal
import dask.dataframe as dd
from datetime import timedelta

from dask_sql.mappings import python_to_sql_type, sql_to_python_value, sql_to_python_df


def test_python_to_sql():
    assert str(python_to_sql_type(np.dtype("int32"))) == "INTEGER"
    assert str(python_to_sql_type(np.dtype(">M8[ns]"))) == "TIMESTAMP"


def test_sql_to_python():
    assert sql_to_python_value("CHAR(5)", "test 123") == "test 123"
    assert type(sql_to_python_value("BIGINT", 653)) == np.int64
    assert sql_to_python_value("BIGINT", 653) == 653
    assert sql_to_python_value("INTERVAL", 4) == timedelta(milliseconds=4)


def test_sql_to_python_df():
    def to_dd(s):
        return dd.from_pandas(s, npartitions=1)

    assert_series_equal(
        sql_to_python_df("BIGINT", to_dd(pd.Series([653], dtype="int16"))).compute(),
        pd.Series([653], dtype="int64"),
    )
    assert_series_equal(
        sql_to_python_df(
            "DECIMAL(6)", to_dd(pd.Series([1.234], dtype="float64"))
        ).compute(),
        pd.Series([1.234], dtype="float64"),
    )
    assert_series_equal(
        sql_to_python_df("VARCHAR", to_dd(pd.Series([653], dtype="int16"))).compute(),
        pd.Series(["653"], dtype="str"),
    )
    assert_series_equal(
        sql_to_python_df("TIMESTAMP(0)", to_dd(pd.Series(["2020-01-01"]))).compute(),
        pd.Series(pd.to_datetime(["2020-01-01"])),
    )


def test_python_to_sql_to_python():
    assert (
        type(sql_to_python_value(str(python_to_sql_type(np.dtype("int64"))), 54))
        == np.int64
    )
