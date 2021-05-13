import logging
from collections import namedtuple
from typing import Any, Callable, List, Optional, Tuple

import dask.dataframe as dd
import numpy as np
import pandas as pd
from pandas.core.window.indexers import BaseIndexer

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.java import org
from dask_sql.physical.rex.base import BaseRexPlugin
from dask_sql.physical.rex.convert import RexConverter
from dask_sql.physical.rex.core.literal import RexLiteralPlugin
from dask_sql.physical.utils.groupby import get_groupby_with_nulls_cols
from dask_sql.physical.utils.map import map_on_partition_index
from dask_sql.physical.utils.sort import sort_partition_func
from dask_sql.utils import (
    LoggableDataFrame,
    make_pickable_without_dask_sql,
    new_temporary_column,
)

logger = logging.getLogger(__name__)


class OverOperation:
    def __call__(self, partitioned_group, *args) -> pd.Series:
        """Call the stored function"""
        return self.call(partitioned_group, *args)


class FirstValueOperation(OverOperation):
    def call(self, partitioned_group, value_col):
        return partitioned_group[value_col].apply(lambda x: x.iloc[0])


class LastValueOperation(OverOperation):
    def call(self, partitioned_group, value_col):
        return partitioned_group[value_col].apply(lambda x: x.iloc[-1])


class SumOperation(OverOperation):
    def call(self, partitioned_group, value_col):
        return partitioned_group[value_col].sum()


class CountOperation(OverOperation):
    def call(self, partitioned_group, value_col=None):
        if value_col is None:
            return partitioned_group.count().iloc[:, 0].fillna(0)
        else:
            return partitioned_group[value_col].count().fillna(0)


class MaxOperation(OverOperation):
    def call(self, partitioned_group, value_col):
        return partitioned_group[value_col].max()


class MinOperation(OverOperation):
    def call(self, partitioned_group, value_col):
        return partitioned_group[value_col].min()


class BoundDescription(
    namedtuple(
        "BoundDescription",
        ["is_unbounded", "is_preceding", "is_following", "is_current_row", "offset"],
    )
):
    """
    Small helper class to wrap a org.apache.calcite.rex.RexWindowBounds
    Java object, as we can not ship it to to the dask workers
    """

    pass


def to_bound_description(
    java_window: org.apache.calcite.rex.RexWindowBounds,
) -> BoundDescription:
    offset = java_window.getOffset()
    if offset:
        offset = int(RexLiteralPlugin().convert(offset, None, None))
    else:
        offset = None

    return BoundDescription(
        is_unbounded=bool(java_window.isUnbounded()),
        is_preceding=bool(java_window.isPreceding()),
        is_following=bool(java_window.isFollowing()),
        is_current_row=bool(java_window.isCurrentRow()),
        offset=offset,
    )


class Indexer(BaseIndexer):
    """
    Window description used for complex windows with arbitrary start and end.
    This class is directly taken from the fugue project.
    """

    def __init__(self, start: int, end: int):
        super().__init__(self, start=start, end=end)

    def get_window_bounds(
        self,
        num_values: int = 0,
        min_periods: Optional[int] = None,
        center: Optional[bool] = None,
        closed: Optional[str] = None,
    ) -> Tuple[np.ndarray, np.ndarray]:
        if self.start is None:
            start = np.zeros(num_values, dtype=np.int64)
        else:
            start = np.arange(self.start, self.start + num_values, dtype=np.int64)
            if self.start < 0:
                start[: -self.start] = 0
            elif self.start > 0:
                start[-self.start :] = num_values
        if self.end is None:
            end = np.full(num_values, num_values, dtype=np.int64)
        else:
            end = np.arange(self.end + 1, self.end + 1 + num_values, dtype=np.int64)
            if self.end > 0:
                end[-self.end :] = num_values
            elif self.end < 0:
                end[: -self.end] = 0
            else:  # pragma: no cover
                raise AssertionError(
                    "This case should have been handled before! Please report this bug"
                )
        return start, end


class RexOverPlugin(BaseRexPlugin):
    """
    A RexOver is an expression, which calculates a given function over the dataframe
    while first optionally partitoning the data and optionally sorting it.

    expressions like `F OVER (PARTITION BY x ORDER BY y)` apply f on each
    partition separately and sort by y before applying f. The result of this
    calculation has however the same length as the input dataframe - it is not an aggregation.
    Typical examples include ROW_NUMBER and lagging.
    """

    class_name = "org.apache.calcite.rex.RexOver"

    OPERATION_MAPPING = {
        "row_number": None,  # That is the easiest one: we do not even need to have any windowing. We therefore threat it separately
        "$sum0": SumOperation(),
        "sum": SumOperation(),
        # Is replaced by a sum and count by calcite: "avg": ExplodedOperation(AvgOperation()),
        "count": CountOperation(),
        "max": MaxOperation(),
        "min": MinOperation(),
        "single_value": FirstValueOperation(),
        "first_value": FirstValueOperation(),
        "last_value": LastValueOperation(),
    }

    def convert(
        self,
        rex: "org.apache.calcite.rex.RexNode",
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> Any:
        window = rex.getWindow()

        df = dc.df
        cc = dc.column_container

        # Store the divisions to apply them later again
        known_divisions = df.divisions

        # Store the index and sort order to apply them later again
        df, partition_col, index_col, sort_col = self._preserve_index_and_sort(df)
        dc = DataContainer(df, cc)

        # Now extract the groupby and order information
        sort_columns, sort_ascending, sort_null_first = self._extract_ordering(
            window, cc
        )
        logger.debug(
            "Before applying the function, sorting according to {sort_columns}."
        )

        df, group_columns = self._extract_groupby(df, window, dc, context)
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

        logger.debug(f"Executing {operator_name} on {str(LoggableDataFrame(df))}")

        # TODO: can be optimized by re-using already present columns
        operands = [
            RexConverter.convert(o, dc, context=context) for o in rex.getOperands()
        ]

        df, new_column_name = self._apply_function_over(
            df,
            operation,
            operands,
            window,
            group_columns,
            sort_columns,
            sort_ascending,
            sort_null_first,
        )

        # Revert back any sorting and grouping by using the previously stored information
        df = self._revert_partition_and_order(
            df, partition_col, index_col, sort_col, known_divisions
        )

        return df[new_column_name]

    def _preserve_index_and_sort(
        self, df: dd.DataFrame
    ) -> Tuple[dd.DataFrame, str, str, str]:
        """Store the partition number, index and sort order separately to make any shuffling reversible"""
        partition_col, index_col, sort_col = (
            new_temporary_column(df),
            new_temporary_column(df),
            new_temporary_column(df),
        )

        def store_index_columns(partition, partition_index):
            return partition.assign(
                **{
                    partition_col: partition_index,
                    index_col: partition.index,
                    sort_col: range(len(partition)),
                }
            )

        df = map_on_partition_index(df, store_index_columns)

        return df, partition_col, index_col, sort_col

    def _extract_groupby(
        self,
        df: dd.DataFrame,
        window: org.apache.calcite.rex.RexWindow,
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> Tuple[dd.DataFrame, str]:
        """Prepare grouping columns we can later use while applying the main function"""
        partition_keys = list(window.partitionKeys)
        if partition_keys:
            group_columns = [
                RexConverter.convert(o, dc, context=context) for o in partition_keys
            ]
            group_columns = get_groupby_with_nulls_cols(df, group_columns)
            group_columns = {
                new_temporary_column(df): group_col for group_col in group_columns
            }
        else:
            group_columns = {new_temporary_column(df): 1}

        df = df.assign(**group_columns)
        group_columns = list(group_columns.keys())

        return df, group_columns

    def _extract_ordering(
        self, window: org.apache.calcite.rex.RexWindow, cc: ColumnContainer
    ) -> Tuple[str, str, str]:
        """Prepare sorting information we can later use while applying the main function"""
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
        df: dd.DataFrame,
        f: Callable,
        operands: List[dd.Series],
        window: org.apache.calcite.rex.RexWindow,
        group_columns: List[str],
        sort_columns: List[str],
        sort_ascending: List[bool],
        sort_null_first: List[bool],
    ) -> Tuple[dd.DataFrame, str]:
        """Apply the given function over the dataframe, possibly grouped and sorted per group"""
        temporary_operand_columns = {
            new_temporary_column(df): operand for operand in operands
        }
        df = df.assign(**temporary_operand_columns)
        # Important: move as few bytes as possible to the pickled function,
        # which is evaluated on the workers
        temporary_operand_columns = temporary_operand_columns.keys()

        # Extract the window definition
        lower_bound = to_bound_description(window.getLowerBound())
        upper_bound = to_bound_description(window.getUpperBound())

        new_column_name = new_temporary_column(df)

        @make_pickable_without_dask_sql
        def map_on_each_group(partitioned_group):
            # Apply sorting
            if sort_columns:
                partitioned_group = sort_partition_func(
                    partitioned_group, sort_columns, sort_ascending, sort_null_first
                )

            if f is None:
                # This is the row_number operator.
                # We do not need to do any windowing
                column_result = range(1, len(partitioned_group) + 1)
            else:
                # In all other cases, apply the windowing operation
                if lower_bound.is_unbounded and (
                    upper_bound.is_current_row or upper_bound.offset == 0
                ):
                    windowed_group = partitioned_group.expanding(min_periods=0)
                elif lower_bound.is_preceding and (
                    upper_bound.is_current_row or upper_bound.offset == 0
                ):
                    windowed_group = partitioned_group.rolling(
                        window=lower_bound.offset + 1, min_periods=0,
                    )
                else:
                    lower_offset = (
                        lower_bound.offset if not lower_bound.is_current_row else 0
                    )
                    if lower_bound.is_preceding and lower_offset is not None:
                        lower_offset *= -1
                    upper_offset = (
                        upper_bound.offset if not upper_bound.is_current_row else 0
                    )
                    if upper_bound.is_preceding and upper_offset is not None:
                        upper_offset *= -1

                    indexer = Indexer(lower_offset, upper_offset)
                    windowed_group = partitioned_group.rolling(
                        window=indexer, min_periods=0
                    )

                column_result = f(windowed_group, *temporary_operand_columns)

            partitioned_group = partitioned_group.assign(
                **{new_column_name: column_result}
            )

            return partitioned_group

        # Currently, pandas will always return a float for windowing operations
        meta = df._meta_nonempty.assign(**{new_column_name: 0.0})

        df = df.groupby(group_columns).apply(map_on_each_group, meta=meta)

        return df, new_column_name

    def _revert_partition_and_order(
        self,
        df: dd.DataFrame,
        partition_col: str,
        index_col: str,
        sort_col: str,
        known_divisions: Any,
    ) -> dd.DataFrame:
        """Use the stored information to make revert the shuffling"""
        from dask.dataframe.shuffle import set_partition

        divisions = tuple(range(len(known_divisions)))
        df = set_partition(df, partition_col, divisions)
        df = df.map_partitions(
            lambda x: x.set_index(index_col, drop=True).sort_values(sort_col),
            meta=df._meta.set_index(index_col),
        )
        df.divisions = known_divisions

        return df
