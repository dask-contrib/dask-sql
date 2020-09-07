import numpy as np
from datetime import timedelta

from dask_sql.mappings import python_to_sql_type, sql_to_python_value


def test_python_to_sql():
    assert str(python_to_sql_type(np.dtype("int32"))) == "INTEGER"
    assert str(python_to_sql_type(np.dtype(">M8[ns]"))) == "TIMESTAMP"


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
