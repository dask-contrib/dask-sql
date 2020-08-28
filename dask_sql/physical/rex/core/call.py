import operator
from collections import namedtuple
from functools import reduce
from typing import Any, Union, Callable
import re

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


class LikeOperation(Operation):
    """The like operator (regex for SQL with some twist)"""

    def __init__(self):
        super().__init__(self.like)

    def like(
        self, test: Union[dd.Series, Any], regex: str, escape: str = None,
    ) -> Union[dd.Series, Any]:
        """
        Returns true, if the string test matches the given regex
        (maybe escaped by escape)
        """

        if not escape:
            escape = "\\"

        # Unfortunately, SQL's like syntax is not directly
        # a regular expression. We need to do some translation
        # SQL knows about the following wildcards:
        # %, ?, [], _, #
        transformed_regex = ""
        escaped = False
        in_char_range = False
        for char in regex:
            # Escape characters with "\"
            if escaped:
                char = "\\" + char
                escaped = False

            # Keep character ranges [...] as they are
            elif in_char_range:
                if char == "]":
                    in_char_range = False

            elif char == "[":
                in_char_range = True

            # These chars have a special meaning in regex
            # whereas in SQL they have not, so we need to
            # add additional escaping
            elif char in [
                "#",
                "$",
                "^",
                ".",
                "|",
                "~",
                "-",
                "+",
                "*",
                "?",
                "(",
                ")",
                "{",
                "}",
            ]:
                char = "\\" + char

            # The needed "\" is printed above, so we continue
            elif char == escape:
                escaped = True
                continue

            # An unescaped "%" in SQL is a .*
            elif char == "%":
                char = ".*"

            # An unescaped "_" in SQL is a .
            elif char == "_":
                char = "."

            transformed_regex += char

        # the SQL like always goes over the full string
        transformed_regex = "^" + transformed_regex + "$"

        # Finally, apply the string
        if is_frame(test):
            return test.str.match(transformed_regex)
        else:  # pragma: no cover
            return bool(re.match(transformed_regex, test))


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
        "LIKE": LikeOperation(),
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
