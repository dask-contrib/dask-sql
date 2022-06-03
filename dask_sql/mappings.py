import logging
from datetime import timedelta
from typing import Any

import dask.array as da
import dask.dataframe as dd
import numpy as np
import pandas as pd

from dask_sql._compat import FLOAT_NAN_IMPLEMENTED
from dask_sql.java import org

logger = logging.getLogger(__name__)
SqlTypeName = org.apache.calcite.sql.type.SqlTypeName

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
    pd.UInt64Dtype(): SqlTypeName.BIGINT,
    np.uint32: SqlTypeName.INTEGER,
    pd.UInt32Dtype(): SqlTypeName.INTEGER,
    np.uint16: SqlTypeName.SMALLINT,
    pd.UInt16Dtype(): SqlTypeName.SMALLINT,
    np.uint8: SqlTypeName.TINYINT,
    pd.UInt8Dtype(): SqlTypeName.TINYINT,
    np.bool8: SqlTypeName.BOOLEAN,
    pd.BooleanDtype(): SqlTypeName.BOOLEAN,
    np.object_: SqlTypeName.VARCHAR,
    pd.StringDtype(): SqlTypeName.VARCHAR,
    np.datetime64: SqlTypeName.TIMESTAMP,
}

if FLOAT_NAN_IMPLEMENTED:  # pragma: no cover
    _PYTHON_TO_SQL.update(
        {pd.Float32Dtype(): SqlTypeName.FLOAT, pd.Float64Dtype(): SqlTypeName.FLOAT}
    )

# Default mapping between SQL types and python types
# for values
_SQL_TO_PYTHON_SCALARS = {
    "DOUBLE": np.float64,
    "FLOAT": np.float32,
    "DECIMAL": np.float32,
    "BIGINT": np.int64,
    "INTEGER": np.int32,
    "SMALLINT": np.int16,
    "TINYINT": np.int8,
    "BOOLEAN": np.bool8,
    "VARCHAR": str,
    "CHAR": str,
    "NULL": type(None),
    "SYMBOL": lambda x: x,  # SYMBOL is a special type used for e.g. flags etc. We just keep it
}

# Default mapping between SQL types and python types
# for data frames
_SQL_TO_PYTHON_FRAMES = {
    "DOUBLE": np.float64,
    "FLOAT": np.float32,
    "DECIMAL": np.float64,
    "BIGINT": pd.Int64Dtype(),
    "INTEGER": pd.Int32Dtype(),
    "INT": pd.Int32Dtype(),  # Although not in the standard, makes compatibility easier
    "SMALLINT": pd.Int16Dtype(),
    "TINYINT": pd.Int8Dtype(),
    "BOOLEAN": pd.BooleanDtype(),
    "VARCHAR": pd.StringDtype(),
    "CHAR": pd.StringDtype(),
    "STRING": pd.StringDtype(),  # Although not in the standard, makes compatibility easier
    "DATE": np.dtype(
        "<M8[ns]"
    ),  # TODO: ideally this would be np.dtype("<M8[D]") but that doesn't work for Pandas
    "TIMESTAMP": np.dtype("<M8[ns]"),
    "NULL": type(None),
}


def python_to_sql_type(python_type):
    """Mapping between python and SQL types."""

    if isinstance(python_type, np.dtype):
        python_type = python_type.type

    if pd.api.types.is_datetime64tz_dtype(python_type):
        return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE

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

    if (
        sql_type.startswith("CHAR(")
        or sql_type.startswith("VARCHAR(")
        or sql_type == "VARCHAR"
        or sql_type == "CHAR"
    ):
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
        # check for finer granular interval types, e.g., INTERVAL MONTH, INTERVAL YEAR
        try:
            interval_type = sql_type.split()[1].lower()

            if interval_type in {"year", "quarter", "month"}:
                # if sql_type is INTERVAL YEAR, Calcite will covert to months
                delta = pd.tseries.offsets.DateOffset(months=float(str(literal_value)))
                return delta
        except IndexError:  # pragma: no cover
            # no finer granular interval type specified
            pass
        except TypeError:  # pragma: no cover
            # interval type is not recognized, fall back to default case
            pass

        # Calcite will always convert INTERVAL types except YEAR, QUATER, MONTH to milliseconds
        # Issue: if sql_type is INTERVAL MICROSECOND, and value <= 1000, literal_value will be rounded to 0
        return timedelta(milliseconds=float(str(literal_value)))

    elif sql_type == "BOOLEAN":
        return bool(literal_value)

    elif (
        sql_type.startswith("TIMESTAMP(")
        or sql_type.startswith("TIME(")
        or sql_type == "DATE"
    ):
        if str(literal_value) == "None":
            # NULL time
            return pd.NaT  # pragma: no cover

        tz = literal_value.getTimeZone().getID()
        assert str(tz) == "UTC", "The code can currently only handle UTC timezones"

        dt = np.datetime64(literal_value.getTimeInMillis(), "ms")

        if sql_type == "DATE":
            return dt.astype("<M8[D]")
        return dt.astype("<M8[ns]")
    elif sql_type.startswith("DECIMAL("):
        # We use np.float64 always, even though we might
        # be able to use a smaller type
        python_type = np.float64
    else:
        try:
            python_type = _SQL_TO_PYTHON_SCALARS[sql_type]
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


def sql_to_python_type(sql_type: str) -> type:
    """Turn an SQL type into a dataframe dtype"""
    if sql_type.startswith("CHAR(") or sql_type.startswith("VARCHAR("):
        return pd.StringDtype()
    elif sql_type.startswith("INTERVAL"):
        return np.dtype("<m8[ns]")

    elif sql_type.startswith("TIMESTAMP(") or sql_type.startswith("TIME("):
        return np.dtype("<M8[ns]")
    elif sql_type.startswith("TIMESTAMP_WITH_LOCAL_TIME_ZONE("):
        # Everything is converted to UTC
        # So far, this did not break
        return pd.DatetimeTZDtype(unit="ns", tz="UTC")
    elif sql_type.startswith("DECIMAL("):
        # We use np.float64 always, even though we might
        # be able to use a smaller type
        return np.float64
    else:
        try:
            return _SQL_TO_PYTHON_FRAMES[sql_type]
        except KeyError:  # pragma: no cover
            raise NotImplementedError(
                f"The SQL type {sql_type} is not implemented (yet)"
            )


def similar_type(lhs: type, rhs: type) -> bool:
    """
    Measure simularity between types.
    Two types are similar, if they both come from the same family,
    e.g. both are ints, uints, floats, strings etc.
    Size or precision is not taken into account.

    TODO: nullability is not checked so far.
    """
    pdt = pd.api.types
    is_uint = pdt.is_unsigned_integer_dtype
    is_sint = pdt.is_signed_integer_dtype
    is_float = pdt.is_float_dtype
    is_object = pdt.is_object_dtype
    is_string = pdt.is_string_dtype
    is_dt_ns = pdt.is_datetime64_ns_dtype
    is_dt_tz = lambda t: is_dt_ns(t) and pdt.is_datetime64tz_dtype(t)
    is_dt_ntz = lambda t: is_dt_ns(t) and not pdt.is_datetime64tz_dtype(t)
    is_td_ns = pdt.is_timedelta64_ns_dtype
    is_bool = pdt.is_bool_dtype

    checks = [
        is_uint,
        is_sint,
        is_float,
        is_object,
        is_string,
        is_dt_tz,
        is_dt_ntz,
        is_td_ns,
        is_bool,
    ]

    for check in checks:
        if check(lhs) and check(rhs):
            return True

    return False


def cast_column_type(
    df: dd.DataFrame, column_name: str, expected_type: type
) -> dd.DataFrame:
    """
    Cast the type of the given column to the expected type,
    if they are far "enough" away.
    This means, a float will never be converted into a double
    or a tinyint into another int - but a string to an integer etc.
    """
    current_type = df[column_name].dtype

    logger.debug(
        f"Column {column_name} has type {current_type}, expecting {expected_type}..."
    )

    casted_column = cast_column_to_type(df[column_name], expected_type)

    if casted_column is not None:
        df[column_name] = casted_column

    return df


def cast_column_to_type(col: dd.Series, expected_type: str):
    """Cast the given column to the expected type"""
    current_type = col.dtype

    if similar_type(current_type, expected_type):
        logger.debug("...not converting.")
        return None

    if pd.api.types.is_integer_dtype(expected_type):
        if pd.api.types.is_float_dtype(current_type):
            logger.debug("...truncating...")
            # Currently "trunc" can not be applied to NA (the pandas missing value type),
            # because NA is a different type. It works with np.NaN though.
            # For our use case, that does not matter, as the conversion to integer later
            # will convert both NA and np.NaN to NA.
            col = da.trunc(col.fillna(value=np.NaN))
        elif pd.api.types.is_timedelta64_dtype(current_type):
            logger.debug(f"Explicitly casting from {current_type} to np.int64")
            return col.astype(np.int64)

    logger.debug(f"Need to cast from {current_type} to {expected_type}")
    return col.astype(expected_type)
