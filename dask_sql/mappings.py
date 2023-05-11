import logging
from typing import Any

import dask.array as da
import dask.config as dask_config
import dask.dataframe as dd
import numpy as np
import pandas as pd

from dask_planner.rust import DaskTypeMap, SqlType

logger = logging.getLogger(__name__)


# Default mapping between python types and SQL types
_PYTHON_TO_SQL = {
    np.float64: SqlType.DOUBLE,
    pd.Float64Dtype(): SqlType.DOUBLE,
    float: SqlType.FLOAT,
    np.float32: SqlType.FLOAT,
    pd.Float32Dtype(): SqlType.FLOAT,
    np.int64: SqlType.BIGINT,
    pd.Int64Dtype(): SqlType.BIGINT,
    int: SqlType.INTEGER,
    np.int32: SqlType.INTEGER,
    pd.Int32Dtype(): SqlType.INTEGER,
    np.int16: SqlType.SMALLINT,
    pd.Int16Dtype(): SqlType.SMALLINT,
    np.int8: SqlType.TINYINT,
    pd.Int8Dtype(): SqlType.TINYINT,
    np.uint64: SqlType.BIGINT,
    pd.UInt64Dtype(): SqlType.BIGINT,
    np.uint32: SqlType.INTEGER,
    pd.UInt32Dtype(): SqlType.INTEGER,
    np.uint16: SqlType.SMALLINT,
    pd.UInt16Dtype(): SqlType.SMALLINT,
    np.uint8: SqlType.TINYINT,
    pd.UInt8Dtype(): SqlType.TINYINT,
    np.bool8: SqlType.BOOLEAN,
    pd.BooleanDtype(): SqlType.BOOLEAN,
    str: SqlType.VARCHAR,
    np.object_: SqlType.VARCHAR,
    pd.StringDtype(): SqlType.VARCHAR,
    np.datetime64: SqlType.TIMESTAMP,
}

# Default mapping between SQL types and python types
# for values
_SQL_TO_PYTHON_SCALARS = {
    "SqlType.DOUBLE": np.float64,
    "SqlType.FLOAT": np.float32,
    "SqlType.DECIMAL": np.float32,
    "SqlType.BIGINT": np.int64,
    "SqlType.INTEGER": np.int32,
    "SqlType.SMALLINT": np.int16,
    "SqlType.TINYINT": np.int8,
    "SqlType.BOOLEAN": np.bool8,
    "SqlType.VARCHAR": str,
    "SqlType.CHAR": str,
    "SqlType.NULL": type(None),
    "SqlType.SYMBOL": lambda x: x,  # SYMBOL is a special type used for e.g. flags etc. We just keep it
}

# Default mapping between SQL types and python types
# for data frames
_SQL_TO_PYTHON_FRAMES = {
    "SqlType.DOUBLE": np.float64,
    "SqlType.FLOAT": np.float32,
    # a column of Decimals in pandas is `object`, but cuDF has a dedicated dtype
    "SqlType.DECIMAL": np.float64,  # We use np.float64 always, even though we might be able to use a smaller type
    "SqlType.BIGINT": pd.Int64Dtype(),
    "SqlType.INTEGER": pd.Int32Dtype(),
    "SqlType.SMALLINT": pd.Int16Dtype(),
    "SqlType.TINYINT": pd.Int8Dtype(),
    "SqlType.BOOLEAN": pd.BooleanDtype(),
    "SqlType.VARCHAR": pd.StringDtype(),
    "SqlType.CHAR": pd.StringDtype(),
    "SqlType.DATE": np.dtype(
        "<M8[ns]"
    ),  # TODO: ideally this would be np.dtype("<M8[D]") but that doesn't work for Pandas
    "SqlType.TIME": np.dtype("<M8[ns]"),
    "SqlType.TIMESTAMP": np.dtype("<M8[ns]"),
    "SqlType.TIMESTAMP_WITH_LOCAL_TIME_ZONE": pd.DatetimeTZDtype(
        unit="ns", tz="UTC"
    ),  # Everything is converted to UTC. So far, this did not break
    "SqlType.INTERVAL_DAY": np.dtype("<m8[ns]"),
    "SqlType.INTERVAL_MONTH_DAY_NANOSECOND": np.dtype("<m8[ns]"),
    "SqlType.NULL": type(None),
}


def python_to_sql_type(python_type) -> "DaskTypeMap":
    """Mapping between python and SQL types."""

    if python_type in (int, float):
        python_type = np.dtype(python_type)
    elif python_type is str:
        python_type = np.dtype("object")

    if isinstance(python_type, np.dtype):
        python_type = python_type.type

    if pd.api.types.is_datetime64tz_dtype(python_type):
        return DaskTypeMap(
            SqlType.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
            unit=str(python_type.unit),
            tz=str(python_type.tz),
        )

    if is_decimal(python_type):
        return DaskTypeMap(
            SqlType.DECIMAL,
            precision=python_type.precision,
            scale=python_type.scale,
        )

    try:
        return DaskTypeMap(_PYTHON_TO_SQL[python_type])
    except KeyError:  # pragma: no cover
        raise NotImplementedError(
            f"The python type {python_type} is not implemented (yet)"
        )


def sql_to_python_value(sql_type: "SqlType", literal_value: Any) -> Any:
    """Mapping between SQL and python values (of correct type)."""
    # In most of the cases, we turn the value first into a string.
    # That might not be the most efficient thing to do,
    # but works for all types (so far)
    # Additionally, a literal type is not used
    # so often anyways.

    logger.debug(
        f"sql_to_python_value -> sql_type: {sql_type} literal_value: {literal_value}"
    )
    if sql_type == SqlType.CHAR or sql_type == SqlType.VARCHAR:
        # Some varchars contain an additional encoding
        # in the format _ENCODING'string'
        literal_value = str(literal_value)
        if literal_value.startswith("_"):
            encoding, literal_value = literal_value.split("'", 1)
            literal_value = literal_value.rstrip("'")
            literal_value = literal_value.encode(encoding=encoding)
            return literal_value.decode(encoding=encoding)

        return literal_value

    elif (
        sql_type == SqlType.DECIMAL
        and dask_config.get("sql.mappings.decimal_support") == "cudf"
    ):
        from decimal import Decimal

        python_type = Decimal

    elif sql_type == SqlType.INTERVAL_DAY:
        return np.timedelta64(literal_value[0], "D") + np.timedelta64(
            literal_value[1], "ms"
        )
    elif sql_type == SqlType.INTERVAL:
        # check for finer granular interval types, e.g., INTERVAL MONTH, INTERVAL YEAR
        try:
            interval_type = str(sql_type).split()[1].lower()

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
        return np.timedelta64(literal_value, "ms")
    # elif sql_type == SqlType.INTERVAL_MONTH_DAY_NANOSECOND:
    #     # DataFusion assumes 30 days per month. Therefore we multiply number of months by 30 and add to days
    #     return np.timedelta64(
    #         (literal_value[0] * 30) + literal_value[1], "D"
    #     ) + np.timedelta64(literal_value[2], "ns")

    elif sql_type == SqlType.BOOLEAN:
        return bool(literal_value)

    elif (
        sql_type == SqlType.TIMESTAMP
        or sql_type == SqlType.TIME
        or sql_type == SqlType.DATE
    ):
        if isinstance(literal_value, str):
            literal_value = np.datetime64(literal_value)
        elif str(literal_value) == "None":
            # NULL time
            return pd.NaT  # pragma: no cover
        if sql_type == SqlType.DATE:
            literal_value = np.datetime64(literal_value, "ns")
            return literal_value.astype("<M8[ns]")
        return literal_value.astype("<M8[ns]")
    else:
        try:
            python_type = _SQL_TO_PYTHON_SCALARS[str(sql_type)]
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


def sql_to_python_type(sql_type: "SqlType", *args) -> type:
    """Turn an SQL type into a dataframe dtype"""
    try:
        if (
            sql_type == SqlType.DECIMAL
            and dask_config.get("sql.mappings.decimal_support") == "cudf"
        ):
            try:
                import cudf
            except ImportError:
                raise ModuleNotFoundError(
                    "Setting `sql.mappings.decimal_support=cudf` requires cudf"
                )
            return cudf.Decimal128Dtype(*args)
        return _SQL_TO_PYTHON_FRAMES[str(sql_type)]
    except KeyError:  # pragma: no cover
        raise NotImplementedError(
            f"The SQL type {str(sql_type)} is not implemented (yet)"
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
        # is_string_dtype considers decimal columns to be string columns
        lambda x: is_string(x) and not is_decimal(x),
        is_dt_tz,
        is_dt_ntz,
        is_td_ns,
        is_bool,
        is_decimal,
    ]

    for check in checks:
        if check(lhs) and check(rhs):
            # check that decimal columns have equal precision/scale
            if check is is_decimal:
                return lhs.precision == rhs.precision and lhs.scale == rhs.scale
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


def is_decimal(dtype):
    """
    Check if dtype is a decimal type
    """
    return "decimal" in str(dtype).lower()
