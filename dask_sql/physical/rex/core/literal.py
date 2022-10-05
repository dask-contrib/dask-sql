import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

import dask.dataframe as dd
import numpy as np

from dask_planner.rust import SqlTypeName
from dask_sql.datacontainer import DataContainer
from dask_sql.mappings import sql_to_python_value
from dask_sql.physical.rex.base import BaseRexPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import Expression, LogicalPlan

logger = logging.getLogger(__name__)


class SargPythonImplementation:
    """
    Apache Calcite comes with a Sarg literal, which stands for the
    "search arguments" (which are later used in a SEARCH call).
    We transform it into a more manageable python object
    by extracting the Java properties.
    """

    class Range:
        """Helper class to represent one of the ranges in a Sarg object"""

        # def __init__(self, range: com.google.common.collect.Range, literal_type: str):
        #     self.lower_endpoint = None
        #     self.lower_open = True
        #     if range.hasLowerBound():
        #         self.lower_endpoint = sql_to_python_value(
        #             literal_type, range.lowerEndpoint()
        #         )
        #         self.lower_open = (
        #             range.lowerBoundType() == com.google.common.collect.BoundType.OPEN
        #         )

        #     self.upper_endpoint = None
        #     self.upper_open = True
        #     if range.hasUpperBound():
        #         self.upper_endpoint = sql_to_python_value(
        #             literal_type, range.upperEndpoint()
        #         )
        #         self.upper_open = (
        #             range.upperBoundType() == com.google.common.collect.BoundType.OPEN
        #         )

        def filter_on(self, series: dd.Series):
            lower_condition = True
            if self.lower_endpoint is not None:
                if self.lower_open:
                    lower_condition = self.lower_endpoint < series
                else:
                    lower_condition = self.lower_endpoint <= series

            upper_condition = True
            if self.upper_endpoint is not None:
                if self.upper_open:
                    upper_condition = self.upper_endpoint > series
                else:
                    upper_condition = self.upper_endpoint >= series

            return lower_condition & upper_condition

        def __repr__(self) -> str:
            return f"Range {self.lower_endpoint} - {self.upper_endpoint}"

    # def __init__(self, java_sarg: org.apache.calcite.util.Sarg, literal_type: str):
    #     self.ranges = [
    #         SargPythonImplementation.Range(r, literal_type)
    #         for r in java_sarg.rangeSet.asRanges()
    #     ]

    def __repr__(self) -> str:
        return ",".join(map(str, self.ranges))


class RexLiteralPlugin(BaseRexPlugin):
    """
    A RexLiteral in an expression stands for a bare single value.
    The task of this class is therefore just to extract this
    value from the java instance and convert it
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
        literal_type = str(rex.getType())

        # Call the Rust function to get the actual value and convert the Rust
        # type name back to a SQL type
        if literal_type == "Boolean":
            try:
                literal_type = SqlTypeName.BOOLEAN
                literal_value = rex.getBoolValue()
            except TypeError:
                literal_type = SqlTypeName.NULL
                literal_value = None
        elif literal_type == "Float32":
            literal_type = SqlTypeName.FLOAT
            literal_value = rex.getFloat32Value()
        elif literal_type == "Float64":
            literal_type = SqlTypeName.DOUBLE
            literal_value = rex.getFloat64Value()
        elif literal_type == "Decimal128":
            literal_type = SqlTypeName.DECIMAL
            value, _, scale = rex.getDecimal128Value()
            literal_value = value / (10**scale)
        elif literal_type == "UInt8":
            literal_type = SqlTypeName.TINYINT
            literal_value = rex.getUInt8Value()
        elif literal_type == "UInt16":
            literal_type = SqlTypeName.SMALLINT
            literal_value = rex.getUInt16Value()
        elif literal_type == "UInt32":
            literal_type = SqlTypeName.INTEGER
            literal_value = rex.getUInt32Value()
        elif literal_type == "UInt64":
            literal_type = SqlTypeName.BIGINT
            literal_value = rex.getUInt64Value()
        elif literal_type == "Int8":
            literal_type = SqlTypeName.TINYINT
            literal_value = rex.getInt8Value()
        elif literal_type == "Int16":
            literal_type = SqlTypeName.SMALLINT
            literal_value = rex.getInt16Value()
        elif literal_type == "Int32":
            literal_type = SqlTypeName.INTEGER
            literal_value = rex.getInt32Value()
        elif literal_type == "Int64":
            literal_type = SqlTypeName.BIGINT
            literal_value = rex.getInt64Value()
        elif literal_type == "Utf8":
            literal_type = SqlTypeName.VARCHAR
            literal_value = rex.getStringValue()
        elif literal_type == "Date32":
            literal_type = SqlTypeName.DATE
            literal_value = np.datetime64(rex.getDate32Value(), "D")
        elif literal_type == "Date64":
            literal_type = SqlTypeName.DATE
            literal_value = np.datetime64(rex.getDate64Value(), "ms")
        elif literal_type == "Time64":
            literal_value = np.datetime64(rex.getTime64Value(), "ns")
            literal_type = SqlTypeName.TIME
        elif literal_type == "Null":
            literal_type = SqlTypeName.NULL
            literal_value = None
        elif literal_type == "IntervalDayTime":
            literal_type = SqlTypeName.INTERVAL_DAY
            literal_value = rex.getIntervalDayTimeValue()
        elif literal_type in {
            "TimestampSecond",
            "TimestampMillisecond",
            "TimestampMicrosecond",
            "TimestampNanosecond",
        }:
            unit_mapping = {
                "TimestampSecond": "s",
                "TimestampMillisecond": "ms",
                "TimestampMicrosecond": "us",
                "TimestampNanosecond": "ns",
            }
            numpy_unit = unit_mapping.get(literal_type)
            literal_value, timezone = rex.getTimestampValue()
            if timezone and timezone != "UTC":
                raise ValueError("Non UTC timezones not supported")
            elif timezone is None:
                literal_value = datetime.fromtimestamp(literal_value // 10**9)
                literal_value = str(literal_value)
            literal_type = SqlTypeName.TIMESTAMP
            literal_value = np.datetime64(literal_value, numpy_unit)
        else:
            raise RuntimeError(
                f"Failed to map literal type {literal_type} to python type in literal.py"
            )

        # if isinstance(literal_value, org.apache.calcite.util.Sarg):
        #     return SargPythonImplementation(literal_value, literal_type)

        python_value = sql_to_python_value(literal_type, literal_value)
        logger.debug(
            f"literal.py python_value: {python_value} or Python type: {type(python_value)}"
        )

        return python_value
