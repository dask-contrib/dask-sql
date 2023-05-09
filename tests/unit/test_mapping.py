from datetime import timedelta

import numpy as np
import pandas as pd
import pytest

from dask_planner.rust import SqlType
from dask_sql.mappings import python_to_sql_type, similar_type, sql_to_python_value


def test_python_to_sql():
    assert str(python_to_sql_type(np.dtype("int32"))) == "INTEGER"
    assert str(python_to_sql_type(np.dtype(">M8[ns]"))) == "TIMESTAMP"
    assert (
        str(python_to_sql_type(pd.DatetimeTZDtype(unit="ns", tz="UTC")))
        == "TIMESTAMP_WITH_LOCAL_TIME_ZONE"
    )


@pytest.mark.gpu
def test_python_decimal_to_sql():
    import cudf

    assert str(python_to_sql_type(cudf.Decimal64Dtype(12, 3))) == "DECIMAL"
    assert str(python_to_sql_type(cudf.Decimal128Dtype(32, 12))) == "DECIMAL"
    assert str(python_to_sql_type(cudf.Decimal32Dtype(5, -2))) == "DECIMAL"


def test_sql_to_python():
    assert sql_to_python_value(SqlType.VARCHAR, "test 123") == "test 123"
    assert type(sql_to_python_value(SqlType.BIGINT, 653)) == np.int64
    assert sql_to_python_value(SqlType.BIGINT, 653) == 653
    assert sql_to_python_value(SqlType.INTERVAL, 4) == timedelta(microseconds=4000)


def test_python_to_sql_to_python():
    assert (
        type(
            sql_to_python_value(python_to_sql_type(np.dtype("int64")).getSqlType(), 54)
        )
        == np.int64
    )


def test_similar_type():
    assert similar_type(np.int64, np.int32)
    assert similar_type(pd.Int64Dtype(), np.int32)
    assert not similar_type(np.uint32, np.int32)
    assert similar_type(np.float32, np.float64)
    assert similar_type(np.object_, str)
