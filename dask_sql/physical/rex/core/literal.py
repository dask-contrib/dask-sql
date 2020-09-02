from typing import Any

import dask.dataframe as dd

from dask_sql.physical.rex.base import BaseRexPlugin
from dask_sql.mappings import sql_to_python_type, null_python_type


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

    def convert(self, rex: "org.apache.calcite.rex.RexNode", df: dd.DataFrame) -> Any:
        literal_type = str(rex.getType())
        python_type = sql_to_python_type(literal_type)

        # We first turn it into a string.
        # That might not be the most efficient thing to do,
        # but works for all types (so far)
        # Additionally, a literal type is not used
        # so often anyways.
        literal_value = str(rex.getValue())

        # empty literal type
        if literal_value == "None":
            return null_python_type(python_type)

        # strings have the format _ENCODING'string'
        if python_type == str and literal_value.startswith("_"):
            encoding, literal_value = literal_value.split("'", 1)
            literal_value = literal_value.rstrip("'")
            literal_value = literal_value.encode(encoding=encoding)
            return literal_value.decode(encoding=encoding)

        # in all other cases we can just return the value as correct type
        return python_type(literal_value)
