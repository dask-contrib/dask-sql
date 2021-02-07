from typing import Any, Tuple, List, Callable
import logging

import dask.dataframe as dd

from dask_sql.physical.rex.base import BaseRexPlugin
from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.java import org
from dask_sql.physical.utils.sort import sort_partition_func
from dask_sql.physical.utils.groupby import get_groupby_with_nulls_cols
from dask_sql.physical.utils.map import map_on_partition_index
from dask_sql.utils import LoggableDataFrame, new_temporary_column


logger = logging.getLogger(__name__)


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

    OPERATION_MAPPING = {"row_number": lambda x: range(1, len(x) + 1)}

    def convert(
        self,
        rex: "org.apache.calcite.rex.RexNode",
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> Any:
        window = rex.getWindow()
        self._assert_simple_window(window)

        df = dc.df
        cc = dc.column_container

        # Store the divisions to apply them later again
        known_divisions = df.divisions

        # Store the index and sort order to apply them later again
        df, partition_col, index_col, sort_col = self._preserve_index_and_sort(df)

        # Now extract the groupby and order information
        sort_columns, sort_ascending, sort_null_first = self._extract_ordering(
            window, cc
        )
        logger.debug(
            "Before applying the function, sorting according to {sort_columns}."
        )

        df, group_columns = self._extract_groupby(df, window, cc)
        logger.debug(
            "Before applying the function, partitioning according to {group_columns}."
        )


        # Finally apply the actual function on each group separately
        operator_name = str(rex.getOperator().getName())
        operator_name = operator_name.lower()

        try:
            operation = self.OPERATION_MAPPING[operator_name]
        except KeyError:
            try:
                operation = context.functions[operator_name]
            except KeyError:  # pragma: no cover
                raise NotImplementedError(f"{operator_name} not (yet) implemented")

        logger.debug(
            f"Executing {operator_name} on {str(LoggableDataFrame(df)}"
        )

        df, new_column_name = self._apply_function_over(
            df, operation, group_columns, sort_columns, sort_ascending, sort_null_first
        )

        # Revert back any sorting and grouping by using the previously stored information
        df = self._revert_partition_and_order(
            df, partition_col, index_col, sort_col, known_divisions
        )

        return df[new_column_name]

    def _assert_simple_window(self, window: org.apache.calcite.rex.RexWindow):
        """Make sure we can actually handle this window type"""
        lower_bound = window.getLowerBound()
        RexWindowBounds = org.apache.calcite.rex.RexWindowBounds
        assert (
            lower_bound == RexWindowBounds.UNBOUNDED_PRECEDING
        ), "Other lower over bounds than UNBOUNDED are currently not implemented"

        upper_bound = window.getUpperBound()
        assert (
            upper_bound == RexWindowBounds.CURRENT_ROW
        ), "Other upper over bounds than CURRENT_ROW are currently not implemented"

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
        cc: ColumnContainer,
    ) -> Tuple[dd.DataFrame, str]:
        """Prepare grouping columns we can later use while applying the main function"""
        partition_keys = list(window.partitionKeys)
        if partition_keys:
            group_column_indices = [int(i.getIndex()) for i in partition_keys]
            group_columns = [
                cc.get_backend_by_frontend_index(i) for i in group_column_indices
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
        group_columns: List[str],
        sort_columns: List[str],
        sort_ascending: List[bool],
        sort_null_first: List[bool],
    ) -> Tuple[dd.DataFrame, str]:
        """Apply the given function over the dataframe, possibly grouped and sorted per group"""

        def map_on_each_group(partitioned_group):
            if sort_columns:
                partitioned_group = sort_partition_func(
                    partitioned_group, sort_columns, sort_ascending, sort_null_first
                )

            partitioned_group = partitioned_group.assign(
                **{new_column_name: f(partitioned_group)}
            )

            return partitioned_group

        new_column_name = new_temporary_column(df)

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
        df = (
            df.groupby(partition_col)
            .apply(lambda x: x.sort_values(sort_col), meta=df._meta)
            .set_index(index_col)
        )

        df.divisions = known_divisions

        return df
