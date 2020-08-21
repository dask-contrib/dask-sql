from dask_sql.mappings import SQL_TO_NP


class RexLiteralPlugin:
    class_name = "org.apache.calcite.rex.RexLiteral"

    def __call__(self, rex, df):
        literal_type = rex.getType()
        np_type = SQL_TO_NP[str(literal_type)]

        literal_value = rex.getValue()
        return np_type(str(literal_value))
