import logging

# from datetime import datetime
from typing import TYPE_CHECKING, Any

from dask_sql.datacontainer import DataContainer

# from dask_sql.mappings import sql_to_python_value
from dask_sql.physical.rex.base import BaseRexPlugin

# import numpy as np


if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import Expression, LogicalPlan

logger = logging.getLogger(__name__)


class RexLiteralPlugin(BaseRexPlugin):
    """
    A RexLiteral in an expression stands for a bare single value.
    The task of this class is therefore just to extract this
    value from the Rust instance and convert it
    into the correct python type.
    It is typically used when specifying a literal in a SQL expression,
    e.g. in a filter.
    """

    class_name = "RexLiteral"

    def convert(
        self,
        rel: "LogicalPlan",
        rex: "Expression",
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> Any:
        # data_type_map = rex.types()
        python_value = rex.python_value()
        # breakpoint()

        # # Retrieve the SQL value from the `Expr` instance.
        # # Value is retrieved based on Arrow DataType
        # if literal_type == "Boolean":
        #     try:
        #         literal_type = SqlType.BOOLEAN
        #         literal_value = rex.getBoolValue()
        #     except TypeError:
        #         literal_type = SqlType.NULL
        #         literal_value = None
        # elif literal_type == "Float32":
        #     literal_type = SqlType.FLOAT
        #     literal_value = rex.getFloat32Value()
        # elif literal_type == "Float64":
        #     literal_type = SqlType.DOUBLE
        #     literal_value = rex.getFloat64Value()
        # elif literal_type == "Decimal128":
        #     literal_type = SqlType.DECIMAL
        #     value, _, scale = rex.getDecimal128Value()
        #     literal_value = value / (10**scale)
        # elif literal_type == "UInt8":
        #     literal_type = SqlType.TINYINT
        #     literal_value = rex.getUInt8Value()
        # elif literal_type == "UInt16":
        #     literal_type = SqlType.SMALLINT
        #     literal_value = rex.getUInt16Value()
        # elif literal_type == "UInt32":
        #     literal_type = SqlType.INTEGER
        #     literal_value = rex.getUInt32Value()
        # elif literal_type == "UInt64":
        #     literal_type = SqlType.BIGINT
        #     literal_value = rex.getUInt64Value()
        # elif literal_type == "Int8":
        #     literal_type = SqlType.TINYINT
        #     literal_value = rex.getInt8Value()
        # elif literal_type == "Int16":
        #     literal_type = SqlType.SMALLINT
        #     literal_value = rex.getInt16Value()
        # elif literal_type == "Int32":
        #     literal_type = SqlType.INTEGER
        #     literal_value = rex.getInt32Value()
        # elif literal_type == "Int64":
        #     literal_type = SqlType.BIGINT
        #     literal_value = rex.getInt64Value()
        # elif literal_type == "Utf8":
        #     literal_type = SqlType.VARCHAR
        #     literal_value = rex.getStringValue()
        # elif literal_type == "Date32":
        #     literal_type = SqlType.DATE
        #     literal_value = np.datetime64(rex.getDate32Value(), "D")
        # elif literal_type == "Date64":
        #     literal_type = SqlType.DATE
        #     literal_value = np.datetime64(rex.getDate64Value(), "ms")
        # elif literal_type == "Time64":
        #     literal_value = np.datetime64(rex.getTime64Value(), "ns")
        #     literal_type = SqlType.TIME
        # elif literal_type == "Null":
        #     literal_type = SqlType.NULL
        #     literal_value = None
        # elif literal_type == "IntervalDayTime":
        #     literal_type = SqlType.INTERVAL_DAY
        #     literal_value = rex.getIntervalDayTimeValue()
        # elif literal_type == "IntervalMonthDayNano":
        #     literal_type = SqlType.INTERVAL_MONTH_DAY_NANOSECOND
        #     literal_value = rex.getIntervalMonthDayNanoValue()
        # elif literal_type in {
        #     "TimestampSecond",
        #     "TimestampMillisecond",
        #     "TimestampMicrosecond",
        #     "TimestampNanosecond",
        # }:
        #     unit_mapping = {
        #         "TimestampSecond": "s",
        #         "TimestampMillisecond": "ms",
        #         "TimestampMicrosecond": "us",
        #         "TimestampNanosecond": "ns",
        #     }
        #     numpy_unit = unit_mapping.get(literal_type)
        #     literal_value, timezone = rex.getTimestampValue()
        #     if timezone and timezone != "UTC":
        #         raise ValueError("Non UTC timezones not supported")
        #     elif timezone is None:
        #         literal_value = datetime.fromtimestamp(literal_value // 10**9)
        #         literal_value = str(literal_value)
        #     literal_type = SqlType.TIMESTAMP
        #     literal_value = np.datetime64(literal_value, numpy_unit)
        # else:
        #     raise RuntimeError(
        #         f"Failed to map literal type {literal_type} to python type in literal.py"
        #     )

        # python_value = sql_to_python_value(literal_type, literal_value)
        # logger.debug(
        #     f"literal.py python_value: {python_value} or Python type: {type(python_value)}"
        # )

        return python_value
