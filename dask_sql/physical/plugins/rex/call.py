from dask_sql.physical.rex import apply_rex_call


class RexCallPlugin:
    class_name = "org.apache.calcite.rex.RexCall"

    def __call__(self, rex, df):
        operator_name = str(rex.getOperator().getName())
        operands = [apply_rex_call(o, df) for o in rex.getOperands()]

        if operator_name == "AND":
            assert len(operands) == 2
            return operands[0] & operands[1]
        if operator_name == ">":
            assert len(operands) == 2
            return operands[0] > operands[1]
        if operator_name == "<":
            assert len(operands) == 2
            return operands[0] < operands[1]
        else:
            raise NotImplementedError(operator_name)
