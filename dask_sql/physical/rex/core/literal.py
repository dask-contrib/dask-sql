from typing import Any, Tuple

import dask.dataframe as dd
import numpy as np

from dask_sql.datacontainer import DataContainer
from dask_sql.mappings import sql_to_python_value
from dask_sql.physical.rex.base import BaseRexPlugin, OutputColumn, ScalarValue


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
    ) -> Tuple[OutputColumn, DataContainer]:
        literal_value = rex.getValue()

        literal_type = str(rex.getType())
        python_value = sql_to_python_value(literal_type, literal_value)

        # We do not change the dataframe here, as we do not know
        # if the literal will actually end up in a column or not
        return ScalarValue(python_value), dc
