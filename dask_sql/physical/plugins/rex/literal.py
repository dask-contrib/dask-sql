import pandas as pd

from dask_sql.mappings import SQL_TO_NP


class RexLiteralPlugin:
    class_name = "org.apache.calcite.rex.RexLiteral"

    def __call__(self, rex, df):
        literal_type = str(rex.getType())
        np_type = SQL_TO_NP[literal_type]

        literal_value = str(rex.getValue())

        if literal_value == "None":
            return None
        else:
            return np_type(literal_value)
