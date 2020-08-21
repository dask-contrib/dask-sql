import operator
from functools import reduce
from collections import namedtuple

from dask_sql.physical.rex import apply_rex_call


Operation = namedtuple("Operation", ["operator", "binary"])


class RexCallPlugin:
    class_name = "org.apache.calcite.rex.RexCall"

    operation_mapping = {
        "AND": Operation(operator=operator.and_, binary=False),
        "OR": Operation(operator=operator.or_, binary=False),
        ">": Operation(operator=operator.gt, binary=True),
        ">=": Operation(operator=operator.ge, binary=True),
        "<": Operation(operator=operator.lt, binary=True),
        "<=": Operation(operator=operator.le, binary=True),
        "=": Operation(operator=operator.eq, binary=True),
        "+": Operation(operator=operator.add, binary=False),
        "-": Operation(operator=operator.sub, binary=False),
        "/": Operation(operator=operator.truediv, binary=False),
        "*": Operation(operator=operator.mul, binary=False),
    }

    def __call__(self, rex, df):
        operator_name = str(rex.getOperator().getName())
        operands = [apply_rex_call(o, df) for o in rex.getOperands()]

        try:
            operation = self.operation_mapping[operator_name]
        except KeyError:
            raise NotImplementedError(f"{operator_name} not (yet) implemented")

        if operation.binary:
            assert len(operands) == 2

        return reduce(operation.operator, operands)
