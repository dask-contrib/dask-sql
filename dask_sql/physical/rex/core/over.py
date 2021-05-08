import logging
from typing import Any, Callable, List, Tuple

import dask.dataframe as dd
import pandas as pd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.java import org
from dask_sql.physical.rex.base import BaseRexPlugin, ColumnReference, OutputColumn
from dask_sql.physical.rex.convert import RexConverter
from dask_sql.physical.utils.groupby import get_groupby_with_nulls_cols
from dask_sql.physical.utils.map import map_on_partition_index
from dask_sql.physical.utils.sort import sort_partition_func
from dask_sql.utils import LoggableDataFrame, new_temporary_column

logger = logging.getLogger(__name__)


class OverOperation:
    def __call__(self, partitioned_group, *args) -> pd.Series:
        """Call the stored function"""
        return self.call(partitioned_group, *args)


class ExplodedOperation(OverOperation):
    def __init__(self, f):
        self.unexploded_f = f

    def call(self, partitioned_group, *args):
        result = self.unexploded_f(partitioned_group, *args)
        return pd.Series(
            [result] * len(partitioned_group), index=partitioned_group.index
        )


class RowNumberOperation(OverOperation):
    def call(self, partitioned_group):
        return range(1, len(partitioned_group) + 1)


class FirstValueOperation(OverOperation):
    def call(self, partitioned_group, value_col):
        return partitioned_group[value_col].iloc[0]


class LastValueOperation(OverOperation):
    def call(self, partitioned_group, value_col):
        return partitioned_group[value_col].iloc[-1]


class SumOperation(OverOperation):
    def call(self, partitioned_group, value_col):
        return partitioned_group[value_col].sum()


class CountOperation(OverOperation):
    def call(self, partitioned_group, value_col=None):
        if value_col is None:
            return partitioned_group.iloc[:, 0].count()
        else:
            return partitioned_group[value_col].count()


class MaxOperation(OverOperation):
    def call(self, partitioned_group, value_col):
        return partitioned_group[value_col].max()


class MinOperation(OverOperation):
    def call(self, partitioned_group, value_col):
        return partitioned_group[value_col].min()


class RexOverPlugin(BaseRexPlugin):
    """
    A RexOver is an expression, which calculates a given function over the dataframe
    while first optionally partitoning the data and optionally sorting it.

    Expressions like `F OVER (PARTITION BY x ORDER BY y)` apply f on each
    partition separately and sort by y before applying f. The result of this
    calculation has however the same length as the input dataframe - it is not an aggregation.
    Typical examples include ROW_NUMBER and lagging.

    This is an example for a RexPlugin, which actually changes the incoming
    dataframe (instead of just returning a single value or adding another column,
    as most RexPlugins do). It reorders the rows and partitions (shuffles)
    to make the calculation of the group by much easier.

    Attention: when using an OVER without any partitioning, this will cause all
    partitions to be merged into a single one!
    """

    class_name = "org.apache.calcite.rex.RexOver"

    OPERATION_MAPPING = {
        "row_number": RowNumberOperation(),
        "$sum0": ExplodedOperation(SumOperation()),
        # Is replaced by a sum and count by calcite: "avg": ExplodedOperation(AvgOperation()),
        "count": ExplodedOperation(CountOperation()),
        "max": ExplodedOperation(MaxOperation()),
        "min": ExplodedOperation(MinOperation()),
        "single_value": ExplodedOperation(FirstValueOperation()),
        "first_value": ExplodedOperation(FirstValueOperation()),
        "last_value": ExplodedOperation(LastValueOperation()),
    }

    def convert(
        self,
        rex: "org.apache.calcite.rex.RexNode",
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> Tuple[OutputColumn, DataContainer]:
        window = rex.getWindow()
        self._assert_simple_window(window)

        # Extract the groupby and order information
        sort_columns, sort_ascending, sort_null_first = self._extract_ordering(
            dc, window
        )
        logger.debug(
            "Before applying the function, sorting according to {sort_columns}."
        )

        dc, group_columns = self._extract_groupby(dc, window, context)
        logger.debug(
            f"Before applying the function, partitioning according to {group_columns}."
        )

        # Finally apply the actual function on each group separately
        operator = rex.getOperator()
        operator_name = str(operator.getName())
        operator_name = operator_name.lower()

        try:
            operation = self.OPERATION_MAPPING[operator_name]
        except KeyError:  # pragma: no cover
            try:
                operation = context.functions[operator_name]
            except KeyError:  # pragma: no cover
                raise NotImplementedError(f"{operator_name} not (yet) implemented")

        logger.debug(f"Executing {operator_name} on {LoggableDataFrame(dc)}")

        operands = []
        for o in rex.getOperands():
            new_column, dc = RexConverter.convert_to_column_reference(
                o, dc, context=context
            )
            operands.append(new_column)

        new_column_name, dc = self._apply_function_over(
            dc,
            operation,
            operands,
            group_columns,
            sort_columns,
            sort_ascending,
            sort_null_first,
        )

        return ColumnReference(new_column_name), dc

    def _assert_simple_window(self, window: org.apache.calcite.rex.RexWindow):
        """Make sure we can actually handle this window type"""
        lower_bound = window.getLowerBound()
        RexWindowBounds = org.apache.calcite.rex.RexWindowBounds
        assert (
            lower_bound == RexWindowBounds.UNBOUNDED_PRECEDING
        ), f"Lower window bound type {lower_bound} is not implemented"

        upper_bound = window.getUpperBound()
        assert upper_bound in [
            RexWindowBounds.CURRENT_ROW,
            RexWindowBounds.UNBOUNDED_FOLLOWING,
        ], f"Lower window bound type {upper_bound} is not implemented"

    def _extract_groupby(
        self,
        dc: DataContainer,
        window: org.apache.calcite.rex.RexWindow,
        context: "dask_sql.Context",
    ) -> Tuple[DataContainer, List[str]]:
        """Prepare grouping columns we can later use while applying the main function"""
        df = dc.df

        partition_keys = list(window.partitionKeys)

        group_columns = []
        for o in partition_keys:
            group_column, dc = RexConverter.convert_to_column_reference(
                o, dc, context=context
            )
            group_columns.append(group_column)

        group_columns = [g.get(dc) for g in group_columns]
        df = dc.df
        group_columns = get_groupby_with_nulls_cols(df, group_columns)
        group_columns = {
            new_temporary_column(df): group_col for group_col in group_columns
        }

        df = df.assign(**group_columns)
        group_columns = list(group_columns.keys())

        cc = dc.column_container
        dc = DataContainer(df, cc)
        return dc, group_columns

    def _extract_ordering(
        self, dc: DataContainer, window: org.apache.calcite.rex.RexWindow,
    ) -> Tuple[str, str, str]:
        """Prepare sorting information we can later use while applying the main function"""
        cc = dc.column_container

        order_keys = list(window.orderKeys)
        sort_columns_indices = [int(i.getKey().getIndex()) for i in order_keys]
        sort_columns = [
            cc.get_backend_by_frontend_index(i) for i in sort_columns_indices
        ]

        ASCENDING = org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING
        FIRST = org.apache.calcite.rel.RelFieldCollation.NullDirection.FIRST
        sort_ascending = [x.getDirection() == ASCENDING for x in order_keys]
        sort_null_first = [x.getNullDirection() == FIRST for x in order_keys]

        return sort_columns, sort_ascending, sort_null_first

    def _apply_function_over(
        self,
        dc: DataContainer,
        f: Callable,
        operands: List[ColumnReference],
        group_columns: List[str],
        sort_columns: List[str],
        sort_ascending: List[bool],
        sort_null_first: List[bool],
    ) -> Tuple[str, DataContainer]:
        """Apply the given function over the dataframe, possibly grouped and sorted per group"""
        # Important: move as few bytes as possible to the pickled function,
        # which is evaluated on the workers
        operand_columns = [o._column_name for o in operands]

        df = dc.df
        new_column_name = new_temporary_column(df)

        def map_on_each_group(partitioned_group):
            if sort_columns:
                partitioned_group = sort_partition_func(
                    partitioned_group, sort_columns, sort_ascending, sort_null_first
                )

            column_result = f(partitioned_group, *operand_columns)
            partitioned_group = partitioned_group.assign(
                **{new_column_name: column_result}
            )

            return partitioned_group

        meta = df._meta_nonempty.assign(**{new_column_name: 0.0})
        df = df.groupby(group_columns).apply(map_on_each_group, meta=meta)

        # Get rid of the multi-index (dask can not work with it well)
        df = df.reset_index(drop=True)

        cc = dc.column_container
        dc = DataContainer(df, cc)
        return new_column_name, dc
