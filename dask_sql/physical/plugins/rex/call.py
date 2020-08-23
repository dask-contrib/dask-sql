import operator
from functools import reduce
from collections import namedtuple

import numpy as np

from dask_sql.physical.rex import apply_rex_call
from dask_sql.utils import is_frame


class Operation:
    def __init__(self, f):
        self.f = f

    def __call__(self, *operands):
        return self.f(*operands)


class ReduceOperation(Operation):
    def __init__(self, operator):
        self.operator = operator

        super().__init__(lambda *operands: reduce(self.operator, operands))


class CaseOperation(Operation):
    def __init__(self):
        super().__init__(self.case)

    def case(self, where, then, other):
        if is_frame(then):
            return then.where(where, other=other)
        elif is_frame(other):
            return other.where(~where, other=then)
        elif is_frame(where):
            tmp = where.apply(lambda x: then, meta=(where.name, type(then)))
            return tmp.where(where, other=other)
        else:
            return then if where else other
        raise NotImplementedError(
            "This case is not yet implemented"
        )  # pragma: no cover


class RexCallPlugin:
    class_name = "org.apache.calcite.rex.RexCall"

    OPERATION_MAPPING = {
        "AND": ReduceOperation(operator=operator.and_),
        "OR": ReduceOperation(operator=operator.or_),
        ">": ReduceOperation(operator=operator.gt),
        ">=": ReduceOperation(operator=operator.ge),
        "<": ReduceOperation(operator=operator.lt),
        "<=": ReduceOperation(operator=operator.le),
        "=": ReduceOperation(operator=operator.eq),
        "+": ReduceOperation(operator=operator.add),
        "-": ReduceOperation(operator=operator.sub),
        "/": ReduceOperation(operator=operator.truediv),
        "*": ReduceOperation(operator=operator.mul),
        "CASE": CaseOperation(),
    }

    def __call__(self, rex, df):
        operator_name = str(rex.getOperator().getName())
        operands = [apply_rex_call(o, df) for o in rex.getOperands()]

        try:
            operation = self.OPERATION_MAPPING[operator_name]
        except KeyError:
            raise NotImplementedError(f"{operator_name} not (yet) implemented")

        # TODO: assert type

        return operation(*operands)
