from datetime import datetime
import operator
from functools import reduce
from typing import Any, Union, Callable
import re
import logging

import pandas as pd
import numpy as np
import dask.dataframe as dd
import dask.array as da
import pandas as pd
from tzlocal import get_localzone

from dask_sql.physical.rex import RexConverter
from dask_sql.physical.rex.base import BaseRexPlugin
from dask_sql.utils import LoggableDataFrame, is_frame
from dask_sql.datacontainer import DataContainer

logger = logging.getLogger(__name__)
SeriesOrScalar = Union[dd.Series, Any]


class Operation:
    """Helper wrapper around a function, which is used as operator"""

    def __init__(self, f: Callable):
        """Init with the given function"""
        self.f = f

    def __call__(self, *operands) -> SeriesOrScalar:
        """Call the stored function"""
        return self.f(*operands)

    def of(self, op: "Operation") -> "Operation":
        """Functional composition"""
        return Operation(lambda x: self(op(x)))


class TensorScalarOperation(Operation):
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


class SQLDivisionOperator(Operation):
    """
    Division is handled differently in SQL and python.
    In python3, it will always preserve the full information, even if starting with
    an integer (so 1/2 = 0.5).
    In SQL, integer division will return an integer again. However, it is not floor division
    (where -1/2 = -1), but truncated division (so -1 / 2 = 0).
    """

    def __init__(self):
        super().__init__(self.div)

    def div(self, lhs, rhs):
        result = lhs / rhs

        lhs_float = pd.api.types.is_float_dtype(lhs)
        rhs_float = pd.api.types.is_float_dtype(rhs)
        if not lhs_float and not rhs_float:
            result = da.trunc(result)

        return result


class CaseOperation(Operation):
    """The case operator (basically an if then else)"""

    def __init__(self):
        super().__init__(self.case)

    def case(
        self, where: SeriesOrScalar, then: SeriesOrScalar, other: SeriesOrScalar,
    ) -> SeriesOrScalar:
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


class IsFalseOperation(Operation):
    """The is false operator"""

    def __init__(self):
        super().__init__(self.false_)

    def false_(self, df: SeriesOrScalar,) -> SeriesOrScalar:
        """
        Returns true where `df` is false (where `df` can also be just a scalar).
        Returns false on nan.
        """
        if is_frame(df):
            return ~df.fillna(True)

        return not pd.isna(df) and df is not None and not np.isnan(df) and not bool(df)


class IsTrueOperation(Operation):
    """The is true operator"""

    def __init__(self):
        super().__init__(self.true_)

    def true_(self, df: SeriesOrScalar,) -> SeriesOrScalar:
        """
        Returns true where `df` is true (where `df` can also be just a scalar).
        Returns false on nan.
        """
        if is_frame(df):
            return df.fillna(False)

        return not pd.isna(df) and df is not None and not np.isnan(df) and bool(df)


class NotOperation(Operation):
    """The not operator"""

    def __init__(self):
        super().__init__(self.not_)

    def not_(self, df: SeriesOrScalar,) -> SeriesOrScalar:
        """
        Returns not `df` (where `df` can also be just a scalar).
        """
        if is_frame(df):
            return ~(df.astype("boolean"))
        else:
            return not df  # pragma: no cover


class IsNullOperation(Operation):
    """The is null operator"""

    def __init__(self):
        super().__init__(self.null)

    def null(self, df: SeriesOrScalar,) -> SeriesOrScalar:
        """
        Returns true where `df` is null (where `df` can also be just a scalar).
        """
        if is_frame(df):
            return df.isna()

        return pd.isna(df) or df is None or np.isnan(df)


class RegexOperation(Operation):
    """An abstract regex operation, which transforms the SQL regex into something python can understand"""

    def __init__(self):
        super().__init__(self.regex)

    def regex(
        self, test: SeriesOrScalar, regex: str, escape: str = None,
    ) -> SeriesOrScalar:
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

            # These chars have a special meaning in regex
            # whereas in SQL they have not, so we need to
            # add additional escaping
            elif char in self.replacement_chars:
                char = "\\" + char

            elif char == "[":
                in_char_range = True

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
            return test.str.match(transformed_regex).astype("boolean")
        else:  # pragma: no cover
            return bool(re.match(transformed_regex, test))


class LikeOperation(RegexOperation):
    replacement_chars = [
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
        "[",
        "]",
    ]


class SimilarOperation(RegexOperation):
    replacement_chars = [
        "#",
        "$",
        "^",
        ".",
        "~",
        "-",
    ]


class PositionOperation(Operation):
    """The position operator (get the position of a string)"""

    def __init__(self):
        super().__init__(self.position)

    def position(self, search, s, start=None):
        """Attention: SQL starts counting at 1"""
        if is_frame(s):
            s = s.str

        if start is None or start <= 0:
            start = 0
        else:
            start -= 1

        return s.find(search, start) + 1


class SubStringOperation(Operation):
    """The substring operator (get a slice of a string)"""

    def __init__(self):
        super().__init__(self.substring)

    def substring(self, s, start, length=None):
        """Attention: SQL starts counting at 1"""
        if start <= 0:
            start = 0
        else:
            start -= 1

        end = length + start if length else None
        if is_frame(s):
            return s.str.slice(start, end)

        if end:
            return s[start:end]
        else:
            return s[start:]


class TrimOperation(Operation):
    """The trim operator (remove occurrences left and right of a string)"""

    def __init__(self):
        super().__init__(self.trim)

    def trim(self, flags, search, s):
        if is_frame(s):
            s = s.str

        if flags == "LEADING":
            strip_call = s.lstrip
        elif flags == "TRAILING":
            strip_call = s.rstrip
        else:
            strip_call = s.strip

        return strip_call(search)


class OverlayOperation(Operation):
    """The overlay operator (replace string according to positions)"""

    def __init__(self):
        super().__init__(self.overlay)

    def overlay(self, s, replace, start, length=None):
        """Attention: SQL starts counting at 1"""
        if start <= 0:
            start = 0
        else:
            start -= 1

        if length is None:
            length = len(replace)
        end = length + start

        if is_frame(s):
            return s.str.slice_replace(start, end, replace)

        s = s[:start] + replace + s[end:]
        return s


class ExtractOperation(Operation):
    def __init__(self):
        super().__init__(self.extract)

    def extract(self, what, df: SeriesOrScalar):
        input_df = df
        if is_frame(df):
            df = df.dt
        else:
            df = pd.to_datetime(df)

        if what == "CENTURY":
            return da.trunc(df.year / 100)
        elif what == "DAY":
            return df.day
        elif what == "DECADE":
            return da.trunc(df.year / 10)
        elif what == "DOW":
            return (df.dayofweek + 1) % 7
        elif what == "DOY":
            return df.dayofyear
        elif what == "HOUR":
            return df.hour
        elif what == "MICROSECOND":
            return df.microsecond
        elif what == "MILLENNIUM":
            return da.trunc(df.year / 1000)
        elif what == "MILLISECOND":
            return da.trunc(1000 * df.microsecond)
        elif what == "MINUTE":
            return df.minute
        elif what == "MONTH":
            return df.month
        elif what == "QUARTER":
            return df.quarter
        elif what == "SECOND":
            return df.second
        elif what == "WEEK":
            return df.week
        elif what == "YEAR":
            return df.year
        else:  # pragma: no cover
            raise NotImplementedError(f"Extraction of {what} is not (yet) implemented.")


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
        "/": ReduceOperation(operation=SQLDivisionOperator()),
        "*": ReduceOperation(operation=operator.mul),
        # special operations
        "case": CaseOperation(),
        "like": LikeOperation(),
        "similar to": SimilarOperation(),
        "not": NotOperation(),
        "is null": IsNullOperation(),
        "is not null": NotOperation().of(IsNullOperation()),
        "is true": IsTrueOperation(),
        "is not true": NotOperation().of(IsTrueOperation()),
        "is false": IsFalseOperation(),
        "is not false": NotOperation().of(IsFalseOperation()),
        "is unknown": IsNullOperation(),
        "is not unknown": NotOperation().of(IsNullOperation()),
        # Unary math functions
        "abs": TensorScalarOperation(lambda x: x.abs(), np.abs),
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
        "round": TensorScalarOperation(lambda x, *ops: x.round(*ops), np.round),
        "sign": Operation(da.sign),
        "sin": Operation(da.sin),
        "tan": Operation(da.tan),
        "truncate": Operation(da.trunc),
        # string operations
        "||": ReduceOperation(operation=operator.add),
        "char_length": TensorScalarOperation(lambda x: x.str.len(), lambda x: len(x)),
        "upper": TensorScalarOperation(lambda x: x.str.upper(), lambda x: x.upper()),
        "lower": TensorScalarOperation(lambda x: x.str.lower(), lambda x: x.lower()),
        "position": PositionOperation(),
        "trim": TrimOperation(),
        "overlay": OverlayOperation(),
        "substring": SubStringOperation(),
        "initcap": TensorScalarOperation(lambda x: x.str.title(), lambda x: x.title()),
        # date/time operations
        "extract": ExtractOperation(),
        "localtime": Operation(lambda *args: pd.Timestamp.now()),
        "localtimestamp": Operation(lambda *args: pd.Timestamp.now()),
        "current_time": Operation(lambda *args: pd.Timestamp.now()),
        "current_date": Operation(lambda *args: pd.Timestamp.now()),
        "current_timestamp": Operation(lambda *args: pd.Timestamp.now()),
    }

    def convert(
        self,
        rex: "org.apache.calcite.rex.RexNode",
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> SeriesOrScalar:
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

        logger.debug(
            f"Executing {operator_name} on {[str(LoggableDataFrame(df)) for df in operands]}"
        )
        return operation(*operands)

        # TODO: We have information on the typing here - we should use it
