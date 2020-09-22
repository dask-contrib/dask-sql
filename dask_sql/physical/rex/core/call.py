import operator
from collections import namedtuple
from functools import reduce
from typing import Any, Union, Callable
import re

import numpy as np
import dask.dataframe as dd
import dask.array as da

from dask_sql.physical.rex import RexConverter
from dask_sql.physical.rex.base import BaseRexPlugin
from dask_sql.utils import is_frame
from dask_sql.datacontainer import DataContainer


class Operation:
    """Helper wrapper around a function, which is used as operator"""

    def __init__(self, f: Callable):
        """Init with the given function"""
        self.f = f

    def __call__(self, *operands) -> Union[dd.Series, Any]:
        """Call the stored function"""
        return self.f(*operands)

    def of(self, op: "Operation") -> "Operation":
        """Functional composition"""
        return Operation(lambda x: self(op(x)))


class TensorScalaOperation(Operation):
    """
    Helper operation to call a function on the input,
    depending if the first is a dataframe or not
    """

    def __init__(self, tensor_f: Callable, scalar_f: Callable = None):
        """Init with the given operation"""
        super().__init__(self.apply)

        self.tensor_f = tensor_f
        self.scalar_f = scalar_f or tensor_f

    def apply(self, *operands):
        """Call the stored functions"""
        if is_frame(operands[0]):
            return self.tensor_f(*operands)

        return self.scalar_f(*operands)


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
            return then if where else other


class IsTrueOperation(Operation):
    """The is true operator"""

    def __init__(self):
        super().__init__(self.true_)

    def true_(self, df: Union[dd.Series, Any],) -> Union[dd.Series, Any]:
        """
        Returns true where `df` is true (where `df` can also be just a scalar).
        """
        if is_frame(df):
            return df.astype(bool)

        return bool(df)


class NotOperation(Operation):
    """The not operator"""

    def __init__(self):
        super().__init__(self.not_)

    def not_(self, df: Union[dd.Series, Any],) -> Union[dd.Series, Any]:
        """
        Returns not `df` (where `df` can also be just a scalar).
        """
        if is_frame(df):
            return ~df
        else:
            return not df  # pragma: no cover


class IsNullOperation(Operation):
    """The is null operator"""

    def __init__(self):
        super().__init__(self.null)

    def null(self, df: Union[dd.Series, Any],) -> Union[dd.Series, Any]:
        """
        Returns true where `df` is null (where `df` can also be just a scalar).
        """
        if is_frame(df):
            return df.isna()

        return df is None or np.isnan(df)


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


class RexCallPlugin(BaseRexPlugin):
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
        # "binary" functions
        "and": ReduceOperation(operation=operator.and_),
        "or": ReduceOperation(operation=operator.or_),
        ">": ReduceOperation(operation=operator.gt),
        ">=": ReduceOperation(operation=operator.ge),
        "<": ReduceOperation(operation=operator.lt),
        "<=": ReduceOperation(operation=operator.le),
        "=": ReduceOperation(operation=operator.eq),
        "<>": ReduceOperation(operation=operator.ne),
        "+": ReduceOperation(operation=operator.add),
        "-": ReduceOperation(operation=operator.sub),
        "/": ReduceOperation(operation=operator.truediv),
        "*": ReduceOperation(operation=operator.mul),
        # special operations
        "case": CaseOperation(),
        "like": LikeOperation(),
        "not": NotOperation(),
        "is null": IsNullOperation(),
        "is not null": NotOperation().of(IsNullOperation()),
        "is true": IsTrueOperation(),
        # Unary math functions
        "abs": TensorScalaOperation(lambda x: x.abs(), np.abs),
        "acos": Operation(da.arccos),
        "asin": Operation(da.arcsin),
        "atan": Operation(da.arctan),
        "atan2": Operation(da.arctan2),
        "cbrt": Operation(da.cbrt),
        "ceil": Operation(da.ceil),
        "cos": Operation(da.cos),
        "cot": Operation(lambda x: 1 / da.tan(x)),
        "degrees": Operation(da.degrees),
        "exp": Operation(da.exp),
        "floor": Operation(da.floor),
        "log10": Operation(da.log10),
        "ln": Operation(da.log),
        # "mod": Operation(da.mod), # needs cast
        "power": Operation(da.power),
        "radians": Operation(da.radians),
        "round": TensorScalaOperation(lambda x, *ops: x.round(*ops), np.round),
        "sign": Operation(da.sign),
        "sin": Operation(da.sin),
        "tan": Operation(da.tan),
        "truncate": Operation(da.trunc),
    }

    def convert(
        self,
        rex: "org.apache.calcite.rex.RexNode",
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> Union[dd.Series, Any]:
        # Prepare the operands by turning the RexNodes into python expressions
        operands = [
            RexConverter.convert(o, dc, context=context) for o in rex.getOperands()
        ]

        # Now use the operator name in the mapping
        operator_name = str(rex.getOperator().getName())
        operator_name = operator_name.lower()

        try:
            operation = self.OPERATION_MAPPING[operator_name]
        except KeyError:
            try:
                operation = context.functions[operator_name].f
            except KeyError:
                raise NotImplementedError(f"{operator_name} not (yet) implemented")

        return operation(*operands)

        # TODO: We have information on the typing here - we should use it
