from datetime import timedelta

import numpy as np
import pandas as pd

from dask_sql.mappings import python_to_sql_type, sql_to_python_value, similar_type


def test_python_to_sql():
    assert str(python_to_sql_type(np.dtype("int32"))) == "INTEGER"
    assert str(python_to_sql_type(np.dtype(">M8[ns]"))) == "TIMESTAMP"
    assert (
        str(python_to_sql_type(pd.DatetimeTZDtype(unit="ns", tz="UTC")))
        == "TIMESTAMP_WITH_LOCAL_TIME_ZONE"
    )


def test_sql_to_python():
    assert sql_to_python_value("CHAR(5)", "test 123") == "test 123"
    assert type(sql_to_python_value("BIGINT", 653)) == np.int64
    assert sql_to_python_value("BIGINT", 653) == 653
    assert sql_to_python_value("INTERVAL", 4) == timedelta(milliseconds=4)


def test_python_to_sql_to_python():
    assert (
        type(sql_to_python_value(str(python_to_sql_type(np.dtype("int64"))), 54))
        == np.int64
    )


def test_similar_type():
    assert similar_type(np.int64, np.int32)
    assert similar_type(pd.Int64Dtype(), np.int32)
    assert not similar_type(np.uint32, np.int32)
    assert similar_type(np.float32, np.float64)
    assert similar_type(np.object_, str)
