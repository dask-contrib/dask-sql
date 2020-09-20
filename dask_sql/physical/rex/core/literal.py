from typing import Any

import numpy as np
import dask.dataframe as dd

from dask_sql.physical.rex.base import BaseRexPlugin
from dask_sql.mappings import sql_to_python_value
from dask_sql.datacontainer import DataContainer


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
        python_value = sql_to_python_value(literal_type, literal_value)

        return python_value
