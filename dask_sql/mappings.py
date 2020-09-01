import numpy as np
from datetime import timedelta

from dask_sql.java import SqlTypeName


# Default mapping between python types and SQL types
_PYTHON_TO_SQL = {
    np.float64: SqlTypeName.DOUBLE,
    np.float32: SqlTypeName.FLOAT,
    np.int64: SqlTypeName.BIGINT,
    np.int32: SqlTypeName.INTEGER,
    np.int16: SqlTypeName.SMALLINT,
    np.int8: SqlTypeName.TINYINT,
    np.uint64: SqlTypeName.BIGINT,
    np.uint32: SqlTypeName.INTEGER,
    np.uint16: SqlTypeName.SMALLINT,
    np.uint8: SqlTypeName.TINYINT,
    np.bool8: SqlTypeName.BOOLEAN,
    np.object_: SqlTypeName.VARCHAR,
    np.datetime64: SqlTypeName.TIMESTAMP,
}

# Default mapping between SQL types and python types
_SQL_TO_PYTHON = {
    "DOUBLE": np.float64,
    "FLOAT": np.float32,
    "BIGINT": np.int64,
    "INTEGER": np.int32,
    "SMALLINT": np.int16,
    "TINYINT": np.int8,
    "BOOLEAN": np.bool8,
    "VARCHAR": str,
    "NULL": type(None),
}


def python_to_sql_type(python_type):
    """Mapping between python and SQL types."""

    try:
        return _PYTHON_TO_SQL[python_type.type]
    except KeyError:  # pragma: no cover
        raise NotImplementedError(
            f"The python type {python_type} is not implemented (yet)"
        )


def sql_to_python_type(sql_type):
    """Mapping between SQL and python types."""
    if sql_type.startswith("CHAR("):
        return str

    if sql_type.startswith("INTERVAL"):
        # Calcite will always convert to milliseconds
        # no matter what the actual interval is
        # I am not sure if this breaks somewhere,
        # but so far it works
        return lambda x: timedelta(milliseconds=int(x))

    if sql_type.startswith("DECIMAL("):
        # We use np.float64 always
        return np.float64

    try:
        return _SQL_TO_PYTHON[sql_type]
    except KeyError:  # pragma: no cover
        raise NotImplementedError(f"The SQL type {sql_type} is not implemented (yet)")


def null_python_type(python_type):
    if np.issubdtype(python_type, np.number):
        return np.nan

    return None
