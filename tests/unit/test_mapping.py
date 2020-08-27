import numpy as np
from datetime import timedelta

from dask_sql.mappings import python_to_sql_type, sql_to_python_type


def test_python_to_sql():
    assert str(python_to_sql_type(np.dtype("int32"))) == "INTEGER"
    assert str(python_to_sql_type(np.dtype(">M8[ns]"))) == "TIMESTAMP"


def test_sql_to_python():
    assert sql_to_python_type("CHAR(5)") == str
    assert sql_to_python_type("BIGINT") == np.int64
    assert sql_to_python_type("INTERVAL")(4) == timedelta(milliseconds=4)


def test_python_to_sql_to_python():
    assert sql_to_python_type(str(python_to_sql_type(np.dtype("int64")))) == np.int64
