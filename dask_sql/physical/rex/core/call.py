import operator
from collections import namedtuple
from functools import reduce
from typing import Any, Union, Callable

import numpy as np
import dask.dataframe as dd

from dask_sql.physical.rex import RexConverter
from dask_sql.utils import is_frame


class Operation:
    """Helper wrapper around a function, which is used as operator"""

    def __init__(self, f: Callable):
        """Init with the given function"""
        self.f = f

    def __call__(self, *operands) -> Union[dd.Series, Any]:
        """Call the stored function"""
        return self.f(*operands)


class ReduceOperation(Operation):
    """Special operator, which is executed by reducing an operation over the input"""

    def __init__(self, operation: Callable):
        self.operation = operation

        super().__init__(lambda *operands: reduce(self.operation, operands))


class CaseOperation(Operation):
    """The case operator (basically an if then else)"""

    def __init__(self):
        super().__init__(self.case)

    def case(
        self,
        where: Union[dd.Series, Any],
        then: Union[dd.Series, Any],
        other: Union[dd.Series, Any],
    ) -> Union[dd.Series, Any]:
        """
        Returns `then` where `where`, else `other`.
        """
        if is_frame(then):
            return then.where(where, other=other)
        elif is_frame(other):
            return other.where(~where, other=then)
        elif is_frame(where):
            # This one is a bit tricky.
            # Everything except "where" are scalars.
            # To make the "df.where" function still usable
            # we create a temporary dataframe with the
            # properties of where (but the content of then).
            tmp = where.apply(lambda x: then, meta=(where.name, type(then)))
            return tmp.where(where, other=other)
        else:
            return then if where else other  # pragma: no cover


class RexCallPlugin:
    """
    RexCall is used for expressions, which calculate something.
    An example is

        SELECT a + b FROM ...

    but also

        a > 3

    Typically, a RexCall has inputs (which can be RexNodes again)
    and calls a function on these inputs.
    The inputs can either be a column or a scalar value.
    """

    class_name = "org.apache.calcite.rex.RexCall"

    OPERATION_MAPPING = {
        "AND": ReduceOperation(operation=operator.and_),
        "OR": ReduceOperation(operation=operator.or_),
        ">": ReduceOperation(operation=operator.gt),
        ">=": ReduceOperation(operation=operator.ge),
        "<": ReduceOperation(operation=operator.lt),
        "<=": ReduceOperation(operation=operator.le),
        "=": ReduceOperation(operation=operator.eq),
        "+": ReduceOperation(operation=operator.add),
        "-": ReduceOperation(operation=operator.sub),
        "/": ReduceOperation(operation=operator.truediv),
        "*": ReduceOperation(operation=operator.mul),
        "CASE": CaseOperation(),
    }

    def convert(
        self, rex: "org.apache.calcite.rex.RexNode", df: dd.DataFrame
    ) -> Union[dd.Series, Any]:
        # Prepare the operands by turning the RexNodes into python expressions
        operands = [RexConverter.convert(o, df) for o in rex.getOperands()]

        # Now use the operator name in the mapping
        operator_name = str(rex.getOperator().getName())

        try:
            operation = self.OPERATION_MAPPING[operator_name]
        except KeyError:
            raise NotImplementedError(f"{operator_name} not (yet) implemented")

        return operation(*operands)

        # TODO: We have information on the typing here - we should use it
