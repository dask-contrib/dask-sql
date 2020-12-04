import operator
from collections import defaultdict
from functools import reduce
from typing import Callable, Dict, List, Tuple, Union
import uuid
import logging

import dask.dataframe as dd

from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import DataContainer, ColumnContainer

logger = logging.getLogger(__name__)


class GroupDatasetDescription:
    """
    Helper class to put dataframes which are filtered according to a specific column
    into a dictionary.
    Applying the same filter twice on the same dataframe does not give different
    dataframes. Therefore we only hash these dataframes according to the column
    they are filtered by.
    """

    def __init__(self, df: dd.DataFrame, filtered_column: str = ""):
        self.df = df
        self.filtered_column = filtered_column

    def __eq__(self, rhs: "GroupDatasetDescription") -> bool:
        """They are equal of they are filtered by the same column"""
        return self.filtered_column == rhs.filtered_column

    def __hash__(self) -> str:
        return hash(self.filtered_column)

    def __repr__(self) -> str:
        return f"GroupDatasetDescription({self.filtered_column})"


# Description of an aggregation in the form of a mapping
# input column -> output column -> aggregation
AggregationDescription = Dict[str, Dict[str, Union[str, dd.Aggregation]]]


class ReduceAggregation(dd.Aggregation):
    """
    A special form of an aggregation, that applies a given operation
    on all elements in a group with "reduce".
    """

    def __init__(self, name: str, operation: Callable):
        series_aggregate = lambda s: s.aggregate(lambda x: reduce(operation, x))

        super().__init__(name, series_aggregate, series_aggregate)


class LogicalAggregatePlugin(BaseRelPlugin):
    """
    A LogicalAggregate is used in GROUP BY clauses, but also
    when aggregating a function over the full dataset.

    In the first case we need to find out which columns we need to
    group over, in the second case we "cheat" and add a 1-column
    to the dataframe, which allows us to reuse every aggregation
    function we already know of.

    The rest is just a lot of column-name-bookkeeping.
    Fortunately calcite will already make sure, that each
    aggregation function will only every be called with a single input
    column (by splitting the inner calculation to a step before).
    """

    class_name = "org.apache.calcite.rel.logical.LogicalAggregate"

    AGGREGATION_MAPPING = {
        "$sum0": "sum",
        "any_value": dd.Aggregation(
            "any_value",
            lambda s: s.sample(n=1).values,
            lambda s0: s0.sample(n=1).values,
        ),
        "avg": "mean",
        "bit_and": ReduceAggregation("bit_and", operator.and_),
        "bit_or": ReduceAggregation("bit_or", operator.or_),
        "bit_xor": ReduceAggregation("bit_xor", operator.xor),
        "count": "count",
        "every": dd.Aggregation("every", lambda s: s.all(), lambda s0: s0.all()),
        "max": "max",
        "min": "min",
        "single_value": "first",
    }

    def convert(
        self, rel: "org.apache.calcite.rel.RelNode", context: "dask_sql.Context"
    ) -> DataContainer:
        (dc,) = self.assert_inputs(rel, 1, context)

        df = dc.df
        cc = dc.column_container

        # We make our life easier with having unique column names
        cc = cc.make_unique()

        # I have no idea what that is, but so far it was always of length 1
        assert len(rel.getGroupSets()) == 1, "Do not know how to handle this case!"

        # Extract the information, which columns we need to group for
        group_column_indices = [int(i) for i in rel.getGroupSet()]
        group_columns = [
            cc.get_backend_by_frontend_index(i) for i in group_column_indices
        ]

        # Always keep an additional column around for empty groups and aggregates
        additional_column_name = str(uuid.uuid4())

        # NOTE: it might be the case that
        # we do not need this additional
        # column, but hopefully adding a single
        # column of 1 is not so problematic...
        df = df.assign(**{additional_column_name: 1})
        cc = cc.add(additional_column_name)
        dc = DataContainer(df, cc)

        # Collect all aggregates
        filtered_aggregations, output_column_order = self._collect_aggregations(
            rel, dc, group_columns, additional_column_name, context
        )

        if not group_columns:
            # There was actually no GROUP BY specified in the SQL
            # Still, this plan can also be used if we need to aggregate something over the full
            # data sample
            # To reuse the code, we just create a new column at the end with a single value
            # It is important to do this after creating the aggregations,
            # as we do not want this additional column to be used anywhere
            group_columns = [additional_column_name]

            logger.debug("Performing full-table aggregation")

        # Now we can perform the aggregates
        # We iterate through all pairs of (possible pre-filtered)
        # dataframes and the aggregations to perform in this data...
        df_agg = None
        for filtered_df_desc, aggregation in filtered_aggregations.items():
            filtered_column = filtered_df_desc.filtered_column
            if filtered_column:
                logger.debug(
                    f"Aggregating {dict(aggregation)} on the data filtered by {filtered_column}"
                )
            else:
                logger.debug(f"Aggregating {dict(aggregation)} on the data")

            # ... we perform the aggregations ...
            filtered_df = filtered_df_desc.df
            # TODO: we could use the type information for
            # pre-calculating the meta information
            filtered_df_agg = filtered_df.groupby(by=group_columns).agg(aggregation)

            # ... fix the column names to a single level ...
            filtered_df_agg.columns = filtered_df_agg.columns.get_level_values(-1)

            # ... and finally concat the new data with the already present columns
            if df_agg is None:
                df_agg = filtered_df_agg
            else:
                df_agg = df_agg.assign(
                    **{col: filtered_df_agg[col] for col in filtered_df_agg.columns}
                )

        # SQL does not care about the index, but we do not want to have any multiindices
        df_agg = df_agg.reset_index(drop=True)

        # Fix the column names and the order of them, as this was messed with during the aggregations
        df_agg.columns = df_agg.columns.get_level_values(-1)
        cc = ColumnContainer(df_agg.columns).limit_to(output_column_order)

        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        dc = DataContainer(df_agg, cc)
        dc = self.fix_dtype_to_row_type(dc, rel.getRowType())
        return dc

    def _collect_aggregations(
        self,
        rel: "org.apache.calcite.rel.RelNode",
        dc: DataContainer,
        group_columns: List[str],
        additional_column_name: str,
        context: "dask_sql.Context",
    ) -> Tuple[
        Dict[GroupDatasetDescription, AggregationDescription], List[int],
    ]:
        """
        Create a mapping of dataframe -> aggregations (in the form input colum, output column, aggregation)
        and the expected order of output columns.
        """
        aggregations = defaultdict(lambda: defaultdict(dict))
        output_column_order = []
        df = dc.df
        cc = dc.column_container

        # SQL needs to copy the old content also. As the values of the group columns
        # are the same for a single group anyways, we just use the first row
        for col in group_columns:
            aggregations[GroupDatasetDescription(df)][col][col] = "first"
            output_column_order.append(col)

        # Now collect all aggregations
        for agg_call in rel.getNamedAggCalls():
            output_col = str(agg_call.getValue())
            expr = agg_call.getKey()

            if expr.hasFilter():
                filter_column = cc.get_backend_by_frontend_index(expr.filterArg)
                filter_expression = df[filter_column]
                filtered_df = df[filter_expression]

                grouped_df = GroupDatasetDescription(filtered_df, filter_column)
            else:
                grouped_df = GroupDatasetDescription(df)

            if expr.isDistinct():
                raise NotImplementedError("DISTINCT is not implemented (yet)")

            aggregation_name = str(expr.getAggregation().getName())
            aggregation_name = aggregation_name.lower()
            try:
                aggregation_function = self.AGGREGATION_MAPPING[aggregation_name]
            except KeyError:
                try:
                    aggregation_function = context.functions[aggregation_name]
                except KeyError:  # pragma: no cover
                    raise NotImplementedError(
                        f"Aggregation function {aggregation_name} not implemented (yet)."
                    )

            inputs = expr.getArgList()
            if len(inputs) == 1:
                input_col = cc.get_backend_by_frontend_index(inputs[0])
            elif len(inputs) == 0:
                input_col = additional_column_name
            else:
                raise NotImplementedError("Can not cope with more than one input")

            aggregations[grouped_df][input_col][output_col] = aggregation_function
            output_column_order.append(output_col)

        return aggregations, output_column_order
