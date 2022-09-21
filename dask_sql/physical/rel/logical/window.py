import logging
from collections import namedtuple
from functools import partial
from typing import TYPE_CHECKING, Callable, List, Optional, Tuple

import dask.dataframe as dd
import numpy as np
import pandas as pd
from pandas.api.indexers import BaseIndexer

from dask_sql._compat import INDEXER_WINDOW_STEP_IMPLEMENTED
from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.rex.convert import RexConverter
from dask_sql.physical.utils.groupby import get_groupby_with_nulls_cols
from dask_sql.physical.utils.sort import sort_partition_func
from dask_sql.utils import (
    LoggableDataFrame,
    make_pickable_without_dask_sql,
    new_temporary_column,
)

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan

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


class AvgOperation(OverOperation):
    def call(self, partitioned_group, value_col):
        return partitioned_group[value_col].mean()


class BoundDescription(
    namedtuple(
        "BoundDescription",
        ["is_unbounded", "is_preceding", "is_following", "is_current_row", "offset"],
    )
):
    """
    Small helper class to wrap a PyWindowFrame
    object. We can directly ship PyWindowFrame to workers in the future
    """

    pass


def to_bound_description(
    windowFrame,
) -> BoundDescription:
    """Convert the PyWindowFrame object to a BoundDescription representation,
    replacing any literals or references to constants"""
    return BoundDescription(
        is_unbounded=bool(windowFrame.isUnbounded()),
        is_preceding=bool(windowFrame.isPreceding()),
        is_following=bool(windowFrame.isFollowing()),
        is_current_row=bool(windowFrame.isCurrentRow()),
        offset=windowFrame.getOffset(),
    )


class Indexer(BaseIndexer):
    """
    Window description used for complex windows with arbitrary start and end.
    This class is directly taken from the fugue project.
    """

    def __init__(self, start: int, end: int):
        super().__init__(self, start=start, end=end)

    def _get_window_bounds(
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

    if INDEXER_WINDOW_STEP_IMPLEMENTED:

        def get_window_bounds(
            self,
            num_values: int = 0,
            min_periods: Optional[int] = None,
            center: Optional[bool] = None,
            closed: Optional[str] = None,
            step: Optional[int] = None,
        ) -> Tuple[np.ndarray, np.ndarray]:
            return self._get_window_bounds(num_values, min_periods, center, closed)

    else:

        def get_window_bounds(
            self,
            num_values: int = 0,
            min_periods: Optional[int] = None,
            center: Optional[bool] = None,
            closed: Optional[str] = None,
        ) -> Tuple[np.ndarray, np.ndarray]:
            return self._get_window_bounds(num_values, min_periods, center, closed)


def map_on_each_group(
    partitioned_group: pd.DataFrame,
    sort_columns: List[str],
    sort_ascending: List[bool],
    sort_null_first: List[bool],
    lower_bound: BoundDescription,
    upper_bound: BoundDescription,
    operations: List[Tuple[Callable, str, List[str]]],
):
    """Internal function mapped on each group of the dataframe after partitioning"""
    # Apply sorting
    if sort_columns:
        partitioned_group = sort_partition_func(
            partitioned_group, sort_columns, sort_ascending, sort_null_first
        )

    # Apply the windowing operation
    if lower_bound.is_unbounded and (
        upper_bound.is_current_row or upper_bound.offset == 0
    ):
        windowed_group = partitioned_group.expanding(min_periods=1)
    elif lower_bound.is_preceding and (
        upper_bound.is_current_row or upper_bound.offset == 0
    ):
        windowed_group = partitioned_group.rolling(
            window=lower_bound.offset + 1,
            min_periods=1,
        )
    else:
        lower_offset = lower_bound.offset if not lower_bound.is_current_row else 0
        if lower_bound.is_preceding and lower_offset is not None:
            lower_offset *= -1
        upper_offset = upper_bound.offset if not upper_bound.is_current_row else 0
        if upper_bound.is_preceding and upper_offset is not None:
            upper_offset *= -1

        indexer = Indexer(lower_offset, upper_offset)
        windowed_group = partitioned_group.rolling(window=indexer, min_periods=1)

    # Calculate the results
    new_columns = {}
    for f, new_column_name, temporary_operand_columns in operations:
        if f is None:
            # This is the row_number operator.
            # We do not need to do any windowing
            column_result = range(1, len(partitioned_group) + 1)
        else:
            column_result = f(windowed_group, *temporary_operand_columns)

        new_columns[new_column_name] = column_result

    # Now apply all columns at once
    partitioned_group = partitioned_group.assign(**new_columns)
    return partitioned_group


class DaskWindowPlugin(BaseRelPlugin):
    """
    A DaskWindow is an expression, which calculates a given function over the dataframe
    while first optionally partitoning the data and optionally sorting it.

    Expressions like `F OVER (PARTITION BY x ORDER BY y)` apply f on each
    partition separately and sort by y before applying f. The result of this
    calculation has however the same length as the input dataframe - it is not an aggregation.
    Typical examples include ROW_NUMBER and lagging.
    """

    class_name = "Window"

    OPERATION_MAPPING = {
        "row_number": None,  # That is the easiest one: we do not even need to have any windowing. We therefore threat it separately
        "$sum0": SumOperation(),
        "sum": SumOperation(),
        "count": CountOperation(),
        "max": MaxOperation(),
        "min": MinOperation(),
        "single_value": FirstValueOperation(),
        "first_value": FirstValueOperation(),
        "last_value": LastValueOperation(),
        "avg": AvgOperation(),
    }

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        (dc,) = self.assert_inputs(rel, 1, context)

        # Output to the right field names right away
        field_names = rel.getRowType().getFieldNames()
        input_column_count = len(dc.column_container.columns)
        for window in rel.window().getGroups():
            dc = self._apply_window(
                rel, window, input_column_count, dc, field_names, context
            )

        # Finally, fix the output schema if needed
        df = dc.df
        cc = dc.column_container
        cc = cc.limit_to(
            cc.columns[input_column_count:] + cc.columns[0:input_column_count]
        )
        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        dc = DataContainer(df, cc)
        dc = self.fix_dtype_to_row_type(dc, rel.getRowType())

        return dc

    def _apply_window(
        self,
        rel,
        window,
        input_column_count: int,
        dc: DataContainer,
        field_names: List[str],
        context: "dask_sql.Context",
    ):
        temporary_columns = []

        df = dc.df
        cc = dc.column_container

        # Now extract the groupby and order information
        sort_columns, sort_ascending, sort_null_first = self._extract_ordering(
            rel, window, cc
        )
        logger.debug(
            f"Before applying the function, sorting according to {sort_columns}."
        )

        df, group_columns = self._extract_groupby(df, rel, window, dc, context)
        logger.debug(
            f"Before applying the function, partitioning according to {group_columns}."
        )
        # TODO: optimize by re-using already present columns
        temporary_columns += group_columns

        operations, df = self._extract_operations(rel, window, df, dc, context)
        for _, _, cols in operations:
            temporary_columns += cols

        newly_created_columns = [new_column for _, new_column, _ in operations]

        logger.debug(f"Will create {newly_created_columns} new columns")

        # Default window bounds when not specified as unbound preceding and current row (if no order by)
        # unbounded preceding and unbounded following if there's an order by
        if not rel.window().getWindowFrame(window):
            lower_bound = BoundDescription(
                is_unbounded=True,
                is_preceding=True,
                is_following=False,
                is_current_row=False,
                offset=None,
            )
            upper_bound = (
                BoundDescription(
                    is_unbounded=False,
                    is_preceding=False,
                    is_following=False,
                    is_current_row=True,
                    offset=None,
                )
                if sort_columns
                else BoundDescription(
                    is_unbounded=True,
                    is_preceding=False,
                    is_following=True,
                    is_current_row=False,
                    offset=None,
                )
            )
        else:
            lower_bound = to_bound_description(
                rel.window().getWindowFrame(window).getLowerBound(),
            )
            upper_bound = to_bound_description(
                rel.window().getWindowFrame(window).getUpperBound(),
            )

        # Apply the windowing operation
        filled_map = partial(
            map_on_each_group,
            sort_columns=sort_columns,
            sort_ascending=sort_ascending,
            sort_null_first=sort_null_first,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            operations=operations,
        )

        # TODO: That is a bit of a hack. We should really use the real column dtype
        meta = df._meta.assign(**{col: 0.0 for col in newly_created_columns})

        df = df.groupby(group_columns).apply(
            make_pickable_without_dask_sql(filled_map), meta=meta
        )
        logger.debug(
            f"Having created a dataframe {LoggableDataFrame(df)} after windowing. Will now drop {temporary_columns}."
        )
        df = df.drop(columns=temporary_columns).reset_index(drop=True)

        dc = DataContainer(df, cc)
        df = dc.df
        cc = dc.column_container

        for c in newly_created_columns:
            # the fields are in the correct order by definition
            field_name = field_names[len(cc.columns) - input_column_count]
            cc = cc.add(field_name, c)
        dc = DataContainer(df, cc)
        logger.debug(
            f"Removed unneeded columns and registered new ones: {LoggableDataFrame(dc)}."
        )
        return dc

    def _extract_groupby(
        self,
        df: dd.DataFrame,
        rel,
        window,
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> Tuple[dd.DataFrame, str]:
        """Prepare grouping columns we can later use while applying the main function"""
        partition_keys = list(rel.window().getPartitionExprs(window))
        if partition_keys:
            group_columns = [
                df[dc.column_container.get_backend_by_frontend_name(o.column_name(rel))]
                for o in partition_keys
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
        self, rel, window, cc: ColumnContainer
    ) -> Tuple[str, str, str]:
        """Prepare sorting information we can later use while applying the main function"""
        logger.debug(
            "Error is about to be encountered, FIX me when bindings are available in subsequent PR"
        )
        # TODO: This was commented out for flake8 CI passing and needs to be handled
        sort_expressions = rel.window().getSortExprs(window)
        sort_columns = [
            cc.get_backend_by_frontend_name(expr.column_name(rel))
            for expr in sort_expressions
        ]
        sort_ascending = [expr.isSortAscending() for expr in sort_expressions]
        sort_null_first = [expr.isSortNullsFirst() for expr in sort_expressions]
        return sort_columns, sort_ascending, sort_null_first

    def _extract_operations(
        self,
        rel,
        window,
        df: dd.DataFrame,
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> List[Tuple[Callable, str, List[str]]]:
        # Finally apply the actual function on each group separately
        operations = []

        # TODO: datafusion returns only window func expression per window
        # This can be optimized in the physical plan to collect all aggs for a given window
        operator_name = rel.window().getWindowFuncName(window).lower()

        try:
            operation = self.OPERATION_MAPPING[operator_name]
        except KeyError:  # pragma: no cover
            try:
                operation = context.schema[context.schema_name].functions[operator_name]
            except KeyError:  # pragma: no cover
                raise NotImplementedError(f"{operator_name} not (yet) implemented")

        logger.debug(f"Executing {operator_name} on {str(LoggableDataFrame(df))}")

        # TODO: can be optimized by re-using already present columns
        temporary_operand_columns = {
            new_temporary_column(df): RexConverter.convert(rel, o, dc, context=context)
            for o in rel.window().getArgs(window)
        }
        df = df.assign(**temporary_operand_columns)
        temporary_operand_columns = list(temporary_operand_columns.keys())

        operations.append(
            (operation, new_temporary_column(df), temporary_operand_columns)
        )

        return operations, df
