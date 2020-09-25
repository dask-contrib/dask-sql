from typing import Any

import numpy as np
import pandas as pd
from datetime import timedelta, datetime, timezone

from dask_sql.java import SqlTypeName


# Default mapping between python types and SQL types
_PYTHON_TO_SQL = {
    np.float64: SqlTypeName.DOUBLE,
    np.float32: SqlTypeName.FLOAT,
    np.int64: SqlTypeName.BIGINT,
    pd.Int64Dtype(): SqlTypeName.BIGINT,
    np.int32: SqlTypeName.INTEGER,
    pd.Int32Dtype(): SqlTypeName.INTEGER,
    np.int16: SqlTypeName.SMALLINT,
    pd.Int16Dtype(): SqlTypeName.SMALLINT,
    np.int8: SqlTypeName.TINYINT,
    pd.Int8Dtype(): SqlTypeName.TINYINT,
    np.uint64: SqlTypeName.BIGINT,
    pd.Int64Dtype(): SqlTypeName.BIGINT,
    np.uint32: SqlTypeName.INTEGER,
    pd.Int32Dtype(): SqlTypeName.INTEGER,
    np.uint16: SqlTypeName.SMALLINT,
    pd.Int16Dtype(): SqlTypeName.SMALLINT,
    np.uint8: SqlTypeName.TINYINT,
    pd.Int8Dtype(): SqlTypeName.TINYINT,
    np.bool8: SqlTypeName.BOOLEAN,
    pd.BooleanDtype(): SqlTypeName.BOOLEAN,
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

    if isinstance(python_type, np.dtype):
        python_type = python_type.type

    try:
        return _PYTHON_TO_SQL[python_type]
    except KeyError:  # pragma: no cover
        raise NotImplementedError(
            f"The python type {python_type} is not implemented (yet)"
        )


def sql_to_python_value(sql_type: str, literal_value: Any) -> Any:
    """Mapping between SQL and python values (of correct type)."""
    # In most of the cases, we turn the value first into a string.
    # That might not be the most efficient thing to do,
    # but works for all types (so far)
    # Additionally, a literal type is not used
    # so often anyways.

    if sql_type.startswith("CHAR(") or sql_type == "VARCHAR":
        # Some varchars contain an additional encoding
        # in the format _ENCODING'string'
        literal_value = str(literal_value)
        if literal_value.startswith("_"):
            encoding, literal_value = literal_value.split("'", 1)
            literal_value = literal_value.rstrip("'")
            literal_value = literal_value.encode(encoding=encoding)
            return literal_value.decode(encoding=encoding)

        return literal_value

    elif sql_type.startswith("INTERVAL"):
        # Calcite will always convert to milliseconds
        # no matter what the actual interval is
        # I am not sure if this breaks somewhere,
        # but so far it works
        return timedelta(milliseconds=int(literal_value))

    elif (
        sql_type.startswith("TIMESTAMP(")
        or sql_type.startswith("TIME(")
        or sql_type == "DATE"
    ):
        if str(literal_value) == "None":
            # NULL time
            return pd.NaT

        tz = literal_value.getTimeZone().getID()
        assert str(tz) == "UTC", "The code can currently only handle UTC timezones"

        dt = datetime.fromtimestamp(
            int(literal_value.getTimeInMillis()) / 1000, timezone.utc
        )

        return dt

    elif sql_type.startswith("DECIMAL("):
        # We use np.float64 always, even though we might
        # be able to use a smaller type
        python_type = np.float64
    else:
        try:
            python_type = _SQL_TO_PYTHON[sql_type]
        except KeyError:  # pragma: no cover
            raise NotImplementedError(
                f"The SQL type {sql_type} is not implemented (yet)"
            )

    literal_value = str(literal_value)

    # empty literal type. We return NaN if possible
    if literal_value == "None":
        if isinstance(python_type(), np.floating):
            return np.NaN
        else:
            return pd.NA

    return python_type(literal_value)
