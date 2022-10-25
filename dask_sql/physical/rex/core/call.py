import logging
import operator
import re
from functools import partial, reduce
from typing import TYPE_CHECKING, Any, Callable, Union

import dask.array as da
import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.base import tokenize
from dask.dataframe.core import Series
from dask.highlevelgraph import HighLevelGraph
from dask.utils import random_state_data

from dask_planner.rust import SqlTypeName
from dask_sql.datacontainer import DataContainer
from dask_sql.mappings import (
    cast_column_to_type,
    sql_to_python_type,
    sql_to_python_value,
)
from dask_sql.physical.rex import RexConverter
from dask_sql.physical.rex.base import BaseRexPlugin
from dask_sql.physical.rex.core.literal import SargPythonImplementation
from dask_sql.utils import (
    LoggableDataFrame,
    convert_to_datetime,
    is_datetime,
    is_frame,
    make_pickable_without_dask_sql,
)

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import Expression, LogicalPlan

logger = logging.getLogger(__name__)
SeriesOrScalar = Union[dd.Series, Any]


def as_timelike(op):
    if isinstance(op, np.int64):
        return np.timedelta64(op, "D")
    elif isinstance(op, str):
        return np.datetime64(op)
    elif pd.api.types.is_datetime64_dtype(op) or isinstance(op, np.timedelta64):
        return op
    else:
        raise ValueError(f"Don't know how to make {type(op)} timelike")


class Operation:
    """Helper wrapper around a function, which is used as operator"""

    # True, if the operation should also get the dataframe passed
    needs_dc = False

    # True, if the operation should also get the REX
    needs_rex = False

    @staticmethod
    def op_needs_dc(op):
        return hasattr(op, "needs_dc") and op.needs_dc

    @staticmethod
    def op_needs_rex(op):
        return hasattr(op, "needs_rex") and op.needs_rex

    def __init__(self, f: Callable):
        """Init with the given function"""
        self.f = f

    def __call__(self, *operands, **kwargs) -> SeriesOrScalar:
        """Call the stored function"""
        return self.f(*operands, **kwargs)

    def of(self, op: "Operation") -> "Operation":
        """Functional composition"""
        new_op = Operation(lambda *x, **kwargs: self(op(*x, **kwargs)))
        new_op.needs_dc = Operation.op_needs_dc(op)
        new_op.needs_rex = Operation.op_needs_rex(op)

        return new_op


class PredicateBasedOperation(Operation):
    """
    Helper operation to call a function on the input,
    depending if the first arg evaluates, given a predicate function, to true or false
    """

    def __init__(
        self, predicate: Callable, true_route: Callable, false_route: Callable
    ):
        super().__init__(self.apply)
        self.predicate = predicate
        self.true_route = true_route
        self.false_route = false_route

    def apply(self, *operands, **kwargs):
        if self.predicate(operands[0]):
            return self.true_route(*operands, **kwargs)

        return self.false_route(*operands, **kwargs)


class TensorScalarOperation(PredicateBasedOperation):
    """
    Helper operation to call a function on the input,
    depending if the first is a dataframe or not
    """

    def __init__(self, tensor_f: Callable, scalar_f: Callable = None):
        """Init with the given operation"""
        super().__init__(is_frame, tensor_f, scalar_f)


class ReduceOperation(Operation):
    """Special operator, which is executed by reducing an operation over the input"""

    def __init__(self, operation: Callable, unary_operation: Callable = None):
        self.operation = operation
        self.unary_operation = unary_operation or operation
        self.needs_dc = Operation.op_needs_dc(self.operation)
        self.needs_rex = Operation.op_needs_rex(self.operation)

        super().__init__(self.reduce)

    def reduce(self, *operands, **kwargs):
        if len(operands) > 1:
            if any(
                map(
                    lambda op: is_frame(op) & pd.api.types.is_datetime64_dtype(op),
                    operands,
                )
            ):
                operands = tuple(map(as_timelike, operands))
            return reduce(partial(self.operation, **kwargs), operands)
        else:
            return self.unary_operation(*operands, **kwargs)


class SQLDivisionOperator(Operation):
    """
    Division is handled differently in SQL and python.
    In python3, it will always preserve the full information, even if starting with
    an integer (so 1/2 = 0.5).
    In SQL, integer division will return an integer again. However, it is not floor division
    (where -1/2 = -1), but truncated division (so -1 / 2 = 0).
    """

    needs_rex = True

    def __init__(self):
        super().__init__(self.div)

    def div(self, lhs, rhs, rex=None):
        result = lhs / rhs

        output_type = str(rex.getType())
        output_type = sql_to_python_type(SqlTypeName.fromString(output_type.upper()))

        is_float = pd.api.types.is_float_dtype(output_type)
        if not is_float:
            result = da.trunc(result)

        return result


class IntDivisionOperator(Operation):
    """
    Truncated integer division (so -1 / 2 = 0).
    This is only used for internal calculations,
    which are created by Calcite.
    """

    def __init__(self):
        super().__init__(self.div)

    def div(self, lhs, rhs):
        result = lhs / rhs

        # Specialized code for literals like "1000µs"
        # For some reasons, Calcite decides to represent
        # 1000µs as 1000µs * 1000 / 1000
        # We do not need to truncate in this case
        # So far, I did not spot any other occurrence
        # of this function.
        if isinstance(result, np.timedelta64):
            return result
        else:
            return da.trunc(result).astype(np.int64)


class CaseOperation(Operation):
    """The case operator (basically an if then else)"""

    def __init__(self):
        super().__init__(self.case)

    def case(self, *operands) -> SeriesOrScalar:
        """
        Returns `then` where `where`, else `other`.
        """
        assert operands

        where = operands[0]
        then = operands[1]

        if len(operands) > 3:
            other = self.case(*operands[2:])
        elif len(operands) == 2:
            # CASE/WHEN statement without an ELSE
            other = None
        else:
            other = operands[2]

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


class CastOperation(Operation):
    """The cast operator"""

    needs_rex = True

    def __init__(self):
        super().__init__(self.cast)

    def cast(self, operand, rex=None) -> SeriesOrScalar:
        output_type = str(rex.getType())
        sql_type = SqlTypeName.fromString(output_type.upper())

        if not is_frame(operand):  # pragma: no cover
            return sql_to_python_value(sql_type, operand)

        python_type = sql_to_python_type(sql_type)

        return_column = cast_column_to_type(operand, python_type)

        if return_column is None:
            return_column = operand

        # TODO: ideally we don't want to directly access the datetimes,
        # but Pandas can't truncate timezone datetimes and cuDF can't
        # truncate datetimes
        if output_type == "DATE":
            return return_column.dt.floor("D").astype(python_type)

        return return_column


class IsFalseOperation(Operation):
    """The is false operator"""

    def __init__(self):
        super().__init__(self.false_)

    def false_(
        self,
        df: SeriesOrScalar,
    ) -> SeriesOrScalar:
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

    def true_(
        self,
        df: SeriesOrScalar,
    ) -> SeriesOrScalar:
        """
        Returns true where `df` is true (where `df` can also be just a scalar).
        Returns false on nan.
        """
        if is_frame(df):
            return df.fillna(False)

        return not pd.isna(df) and df is not None and not np.isnan(df) and bool(df)


class NegativeOperation(Operation):
    """The negative operator"""

    def __init__(self):
        super().__init__(self.negative_)

    def negative_(
        self,
        df: SeriesOrScalar,
    ) -> SeriesOrScalar:
        return -df


class NotOperation(Operation):
    """The not operator"""

    def __init__(self):
        super().__init__(self.not_)

    def not_(
        self,
        df: SeriesOrScalar,
    ) -> SeriesOrScalar:
        """
        Returns not `df` (where `df` can also be just a scalar).
        """
        if is_frame(df):
            return ~(df.astype("boolean"))
        else:
            return not df


class IsNullOperation(Operation):
    """The is null operator"""

    def __init__(self):
        super().__init__(self.null)

    def null(
        self,
        df: SeriesOrScalar,
    ) -> SeriesOrScalar:
        """
        Returns true where `df` is null (where `df` can also be just a scalar).
        """
        if is_frame(df):
            return df.isna()

        return pd.isna(df) or df is None or np.isnan(df)


class IsNotDistinctOperation(Operation):
    """The is not distinct operator"""

    def __init__(self):
        super().__init__(self.not_distinct)

    def not_distinct(self, lhs: SeriesOrScalar, rhs: SeriesOrScalar) -> SeriesOrScalar:
        """
        Returns true where `lhs` is not distinct from `rhs` (or both are null).
        """
        is_null = IsNullOperation()

        return (is_null(lhs) & is_null(rhs)) | (lhs == rhs)


class RegexOperation(Operation):
    """An abstract regex operation, which transforms the SQL regex into something python can understand"""

    needs_rex = True

    def __init__(self):
        super().__init__(self.regex)

    def regex(self, test: SeriesOrScalar, regex: str, rex=None) -> SeriesOrScalar:
        """
        Returns true, if the string test matches the given regex
        (maybe escaped by escape)
        """
        escape = rex.getEscapeChar() if rex else None
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
        else:
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

    def __init__(self, flag="BOTH"):
        self.flag = flag
        super().__init__(self.trim)

    def trim(self, s, search):
        if is_frame(s):
            s = s.str

        if self.flag == "LEADING":
            strip_call = s.lstrip
        elif self.flag == "TRAILING":
            strip_call = s.rstrip
        elif self.flag == "BOTH":
            strip_call = s.strip
        else:
            raise ValueError(f"Trim type {self.flag} not recognized")

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
        df = convert_to_datetime(df)

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
        else:
            raise NotImplementedError(f"Extraction of {what} is not (yet) implemented.")


class YearOperation(Operation):
    def __init__(self):
        super().__init__(self.extract_year)

    def extract_year(self, df: SeriesOrScalar):
        df = convert_to_datetime(df)
        return df.year


class TimeStampAddOperation(Operation):
    def __init__(self):
        super().__init__(self.timestampadd)

    def timestampadd(self, unit, interval, df: SeriesOrScalar):
        df = convert_to_datetime(df)

        if unit in {"DAY", "SQL_TSI_DAY"}:
            return df + np.timedelta64(interval, "D")
        elif unit in {"HOUR", "SQL_TSI_HOUR"}:
            return df + np.timedelta64(interval, "h")
        elif unit == "MICROSECOND":
            return df + np.timedelta64(interval, "us")
        elif unit == "MILLISECOND":
            return df + np.timedelta64(interval, "ms")
        elif unit in {"MINUTE", "SQL_TSI_MINUTE"}:
            return df + np.timedelta64(interval, "m")
        elif unit in {"SECOND", "SQL_TSI_SECOND"}:
            return df + np.timedelta64(interval, "s")
        elif unit in {"WEEK", "SQL_TSI_WEEK"}:
            return df + np.timedelta64(interval * 7, "W")
        else:
            raise NotImplementedError(f"Extraction of {unit} is not (yet) implemented.")


class CeilFloorOperation(PredicateBasedOperation):
    """
    Apply ceil/floor operations on a series depending on its dtype (datetime like vs normal)
    """

    def __init__(self, round_method: str):
        assert round_method in {
            "ceil",
            "floor",
        }, "Round method can only be either ceil or floor"

        super().__init__(
            is_datetime,  # if the series is dt type
            self._round_datetime,
            getattr(da, round_method),
        )

        self.round_method = round_method

    def _round_datetime(self, *operands):
        df, unit = operands

        df = convert_to_datetime(df)

        unit_map = {
            "DAY": "D",
            "HOUR": "H",
            "MINUTE": "T",
            "SECOND": "S",
            "MICROSECOND": "U",
            "MILLISECOND": "L",
        }

        try:
            freq = unit_map[unit.upper()]
            return getattr(df, self.round_method)(freq)
        except KeyError:
            raise NotImplementedError(
                f"{self.round_method} TO {unit} is not (yet) implemented."
            )


class BaseRandomOperation(Operation):
    """
    Return a random number (specified by the given function) with the random number
    generator set to the given seed.
    As we need to know how many random numbers we should generate,
    we also get the current dataframe as input and use it to
    create random numbers for each partition separately.
    To make this deterministic, we use the partition number
    as additional input to the seed.
    """

    needs_dc = True

    def random_function(self, partition, random_state, kwargs):
        """Needs to be implemented in derived classes"""
        raise NotImplementedError

    def random_frame(self, seed: int, dc: DataContainer, **kwargs) -> dd.Series:
        """This function - in contrast to others in this module - will only ever be called on data frames"""

        random_state = np.random.RandomState(seed=seed)

        # Idea taken from dask.DataFrame.sample:
        # initialize a random state for each of the partitions
        # separately and then create a random series
        # for each partition
        df = dc.df
        name = "sample-" + tokenize(df, random_state)

        state_data = random_state_data(df.npartitions, random_state)
        dsk = {
            (name, i): (
                make_pickable_without_dask_sql(self.random_function),
                (df._name, i),
                np.random.RandomState(state),
                kwargs,
            )
            for i, state in enumerate(state_data)
        }

        graph = HighLevelGraph.from_collections(name, dsk, dependencies=[df])
        random_series = Series(graph, name, ("random", "float64"), df.divisions)

        # This part seems to be stupid, but helps us do a very simple
        # task without going into the (private) internals of Dask:
        # copy all meta information from the original input dataframe
        # This is important so that the returned series looks
        # exactly like coming from the input dataframe
        return_df = df.assign(random=random_series)["random"]
        return return_df


class RandOperation(BaseRandomOperation):
    """Create a random number between 0 and 1"""

    def __init__(self):
        super().__init__(f=self.rand)

    def rand(self, seed: int = None, dc: DataContainer = None):
        return self.random_frame(seed=seed, dc=dc)

    def random_function(self, partition, random_state, kwargs):
        return random_state.random_sample(size=len(partition))


class RandIntegerOperation(BaseRandomOperation):
    """Create a random integer between 0 and high"""

    def __init__(self):
        super().__init__(f=self.rand_integer)

    def rand_integer(
        self, seed: int = None, high: int = None, dc: DataContainer = None
    ):
        # Two possibilities: RAND_INTEGER(seed, high) or RAND_INTEGER(high)
        if high is None:
            high = seed
            seed = None
        return self.random_frame(seed=seed, high=high, dc=dc)

    def random_function(self, partition, random_state, kwargs):
        return random_state.randint(size=len(partition), low=0, **kwargs)


class SearchOperation(Operation):
    """
    Search is a special operation in SQL, which allows to write "range-like"
    conditions, such like

        (1 < a AND a < 2) OR (4 < a AND a < 6)

    in a more convenient setting.
    """

    def __init__(self):
        super().__init__(self.search)

    def search(self, series: dd.Series, sarg: SargPythonImplementation):
        conditions = [r.filter_on(series) for r in sarg.ranges]

        assert len(conditions) > 0

        if len(conditions) > 1:
            or_operation = ReduceOperation(operation=operator.or_)
            return or_operation(*conditions)
        else:
            return conditions[0]


class DatePartOperation(Operation):
    """
    Function for performing PostgreSQL like functions in a more convenient setting.
    """

    def __init__(self):
        super().__init__(self.date_part)

    def date_part(self, what, df: SeriesOrScalar):
        what = what.upper()
        df = convert_to_datetime(df)

        if what == "YEAR":
            return df.year

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
        else:
            raise NotImplementedError(f"Extraction of {what} is not (yet) implemented.")


class BetweenOperation(Operation):
    """
    Function for finding rows between two scalar values
    """

    needs_rex = True

    def __init__(self):
        super().__init__(self.between)

    def between(self, series: dd.Series, low, high, rex=None):
        return (
            ~series.between(low, high, inclusive="both")
            if rex.isNegated()
            else series.between(low, high, inclusive="both")
        )


class InListOperation(Operation):
    """
    Returns a boolean of whether an expression is/isn't in a set of values
    """

    needs_rex = True

    def __init__(self):
        super().__init__(self.inList)

    def inList(self, series: dd.Series, *operands, rex=None):
        result = series.isin(operands)
        return ~result if rex.isNegated() else result


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

    class_name = "RexCall"

    OPERATION_MAPPING = {
        # "binary" functions
        "between": BetweenOperation(),
        "and": ReduceOperation(operation=operator.and_),
        "or": ReduceOperation(operation=operator.or_),
        ">": ReduceOperation(operation=operator.gt),
        ">=": ReduceOperation(operation=operator.ge),
        "<": ReduceOperation(operation=operator.lt),
        "<=": ReduceOperation(operation=operator.le),
        "=": ReduceOperation(operation=operator.eq),
        "!=": ReduceOperation(operation=operator.ne),
        "<>": ReduceOperation(operation=operator.ne),
        "+": ReduceOperation(operation=operator.add, unary_operation=lambda x: x),
        "-": ReduceOperation(operation=operator.sub, unary_operation=lambda x: -x),
        "/": ReduceOperation(operation=SQLDivisionOperator()),
        "*": ReduceOperation(operation=operator.mul),
        "is distinct from": NotOperation().of(IsNotDistinctOperation()),
        "is not distinct from": IsNotDistinctOperation(),
        "/int": IntDivisionOperator(),
        # special operations
        "cast": CastOperation(),
        "case": CaseOperation(),
        "not like": NotOperation().of(LikeOperation()),
        "like": LikeOperation(),
        "similar to": SimilarOperation(),
        "negative": NegativeOperation(),
        "not": NotOperation(),
        "in list": InListOperation(),
        "is null": IsNullOperation(),
        "is not null": NotOperation().of(IsNullOperation()),
        "is true": IsTrueOperation(),
        "is not true": NotOperation().of(IsTrueOperation()),
        "is false": IsFalseOperation(),
        "is not false": NotOperation().of(IsFalseOperation()),
        "is unknown": IsNullOperation(),
        "is not unknown": NotOperation().of(IsNullOperation()),
        "rand": RandOperation(),
        "random": RandOperation(),
        "rand_integer": RandIntegerOperation(),
        "search": SearchOperation(),
        # Unary math functions
        "abs": TensorScalarOperation(lambda x: x.abs(), np.abs),
        "acos": Operation(da.arccos),
        "asin": Operation(da.arcsin),
        "atan": Operation(da.arctan),
        "atan2": Operation(da.arctan2),
        "cbrt": Operation(da.cbrt),
        "ceil": CeilFloorOperation("ceil"),
        "cos": Operation(da.cos),
        "cot": Operation(lambda x: 1 / da.tan(x)),
        "degrees": Operation(da.degrees),
        "exp": Operation(da.exp),
        "floor": CeilFloorOperation("floor"),
        "log10": Operation(da.log10),
        "ln": Operation(da.log),
        "mod": Operation(da.mod),
        "power": Operation(da.power),
        "radians": Operation(da.radians),
        "round": TensorScalarOperation(lambda x, *ops: x.round(*ops), np.round),
        "sign": Operation(da.sign),
        "sin": Operation(da.sin),
        "tan": Operation(da.tan),
        "truncate": Operation(da.trunc),
        # string operations
        "||": ReduceOperation(operation=operator.add),
        "concat": ReduceOperation(operation=operator.add),
        "characterlength": TensorScalarOperation(
            lambda x: x.str.len(), lambda x: len(x)
        ),
        "upper": TensorScalarOperation(lambda x: x.str.upper(), lambda x: x.upper()),
        "lower": TensorScalarOperation(lambda x: x.str.lower(), lambda x: x.lower()),
        "position": PositionOperation(),
        "trim": TrimOperation(),
        "ltrim": TrimOperation("LEADING"),
        "rtrim": TrimOperation("TRAILING"),
        "btrim": TrimOperation("BOTH"),
        "overlay": OverlayOperation(),
        "substr": SubStringOperation(),
        "substring": SubStringOperation(),
        "initcap": TensorScalarOperation(lambda x: x.str.title(), lambda x: x.title()),
        # date/time operations
        "extract": ExtractOperation(),
        "localtime": Operation(lambda *args: pd.Timestamp.now()),
        "localtimestamp": Operation(lambda *args: pd.Timestamp.now()),
        "current_time": Operation(lambda *args: pd.Timestamp.now()),
        "current_date": Operation(lambda *args: pd.Timestamp.now()),
        "current_timestamp": Operation(lambda *args: pd.Timestamp.now()),
        "last_day": TensorScalarOperation(
            lambda x: x + pd.tseries.offsets.MonthEnd(1),
            lambda x: convert_to_datetime(x) + pd.tseries.offsets.MonthEnd(1),
        ),
        # Temporary UDF functions that need to be moved after this POC
        "datepart": DatePartOperation(),
        "year": YearOperation(),
        "timestampadd": TimeStampAddOperation(),
    }

    def convert(
        self,
        rel: "LogicalPlan",
        expr: "Expression",
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> SeriesOrScalar:

        # Prepare the operands by turning the RexNodes into python expressions
        operands = [
            RexConverter.convert(rel, o, dc, context=context)
            for o in expr.getOperands()
        ]

        # Now use the operator name in the mapping
        schema_name = context.schema_name
        operator_name = expr.getOperatorName().lower()

        try:
            operation = self.OPERATION_MAPPING[operator_name]
        except KeyError:
            try:
                operation = context.schema[schema_name].functions[operator_name]
            except KeyError:  # pragma: no cover
                raise NotImplementedError(f"{operator_name} not (yet) implemented")

        logger.debug(
            f"Executing {operator_name} on {[str(LoggableDataFrame(df)) for df in operands]}"
        )

        kwargs = {}

        if Operation.op_needs_dc(operation):
            kwargs["dc"] = dc
        if Operation.op_needs_rex(operation):
            kwargs["rex"] = expr

        return operation(*operands, **kwargs)
        # TODO: We have information on the typing here - we should use it
