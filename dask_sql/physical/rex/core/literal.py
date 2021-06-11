from typing import Any

import dask.dataframe as dd
import numpy as np

from dask_sql.datacontainer import DataContainer
from dask_sql.java import com, org
from dask_sql.mappings import sql_to_python_value
from dask_sql.physical.rex.base import BaseRexPlugin


class SargPythonImplementation:
    """
    Apache Calcite comes with a Sarg literal, which stands for the
    "search arguments" (which are later used in a SEARCH call).
    We transform it into a more manageable python object
    by extracting the Java properties.
    """

    class Range:
        """Helper class to represent one of the ranges in a Sarg object"""

        def __init__(self, range: com.google.common.collect.Range, literal_type: str):
            self.lower_endpoint = None
            self.lower_open = True
            if range.hasLowerBound():
                self.lower_endpoint = sql_to_python_value(
                    literal_type, range.lowerEndpoint()
                )
                self.lower_open = (
                    range.lowerBoundType() == com.google.common.collect.BoundType.OPEN
                )

            self.upper_endpoint = None
            self.upper_open = True
            if range.hasUpperBound():
                self.upper_endpoint = sql_to_python_value(
                    literal_type, range.upperEndpoint()
                )
                self.upper_open = (
                    range.upperBoundType() == com.google.common.collect.BoundType.OPEN
                )

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

    def __init__(self, java_sarg: org.apache.calcite.util.Sarg, literal_type: str):
        self.ranges = [
            SargPythonImplementation.Range(r, literal_type)
            for r in java_sarg.rangeSet.asRanges()
        ]

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

    class_name = "org.apache.calcite.rex.RexLiteral"

    def convert(
        self,
        rex: "org.apache.calcite.rex.RexNode",
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> Any:
        literal_value = rex.getValue()

        literal_type = str(rex.getType())

        if isinstance(literal_value, org.apache.calcite.util.Sarg):
            return SargPythonImplementation(literal_value, literal_type)

        python_value = sql_to_python_value(literal_type, literal_value)

        return python_value
