import logging
import operator
from collections import defaultdict
from functools import reduce
from typing import Any, Callable, Dict, List, Tuple, Union

import dask.dataframe as dd
import pandas as pd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.rex.core.call import IsNullOperation
from dask_sql.physical.utils.groupby import get_groupby_with_nulls_cols
from dask_sql.utils import new_temporary_column

logger = logging.getLogger(__name__)


class ReduceAggregation(dd.Aggregation):
    """
    A special form of an aggregation, that applies a given operation
    on all elements in a group with "reduce".
    """

    def __init__(self, name: str, operation: Callable):
        series_aggregate = lambda s: s.aggregate(lambda x: reduce(operation, x))

        super().__init__(name, series_aggregate, series_aggregate)


class AggregationOnPandas(dd.Aggregation):
    """
    A special form of an aggregation, which does not apply the given function
    (given as attribute name) directly to the dask groupby, but
    via the groupby().apply() method. This is needed to call
    functions directly on the pandas dataframes, but should be done
    very carefully (as it is a performance bottleneck).
    """

    def __init__(self, function_name: str):
        def _f(s):
            return s.apply(lambda s0: getattr(s0.dropna(), function_name)())

        super().__init__(function_name, _f, _f)


class AggregationSpecification:
    """
    Most of the aggregations in SQL are already
    implemented 1:1 in dask and can just be called via their name
    (e.g. AVG is the mean). However sometimes those already
    implemented functions only work well for numerical
    functions. This small container class therefore
    can have an additional aggregation function, which is
    valid for non-numerical types.
    """

    def __init__(self, numerical_aggregation, non_numerical_aggregation=None):
        self.numerical_aggregation = numerical_aggregation
        self.non_numerical_aggregation = (
            non_numerical_aggregation or numerical_aggregation
        )


class LogicalAggregatePlugin(BaseRelPlugin):
    """
    A LogicalAggregate is used in GROUP BY clauses, but also
    when aggregating a function over the full dataset.

    In the first case we need to find out which columns we need to
    group over, in the second case we "cheat" and add a 1-column
    to the dataframe, which allows us to reuse every aggregation
    function we already know of.
    As NULLs are not groupable in dask, we handle them special
    by adding a temporary column which is True for all NULL values
    and False otherwise (and also group by it).

    The rest is just a lot of column-name-bookkeeping.
    Fortunately calcite will already make sure, that each
    aggregation function will only every be called with a single input
    column (by splitting the inner calculation to a step before).

    Open TODO: So far we are following the dask default
    to only have a single partition after the group by (which is usual
    a reasonable assumption). It would be nice to control
    these things via HINTs.
    """

    class_name = "org.apache.calcite.rel.logical.LogicalAggregate"

    AGGREGATION_MAPPING = {
        "$sum0": AggregationSpecification("sum", AggregationOnPandas("sum")),
        "any_value": AggregationSpecification(
            dd.Aggregation(
                "any_value",
                lambda s: s.sample(n=1).values,
                lambda s0: s0.sample(n=1).values,
            )
        ),
        "avg": AggregationSpecification("mean", AggregationOnPandas("mean")),
        "bit_and": AggregationSpecification(
            ReduceAggregation("bit_and", operator.and_)
        ),
        "bit_or": AggregationSpecification(ReduceAggregation("bit_or", operator.or_)),
        "bit_xor": AggregationSpecification(ReduceAggregation("bit_xor", operator.xor)),
        "count": AggregationSpecification("count"),
        "every": AggregationSpecification(
            dd.Aggregation("every", lambda s: s.all(), lambda s0: s0.all())
        ),
        "max": AggregationSpecification("max", AggregationOnPandas("max")),
        "min": AggregationSpecification("min", AggregationOnPandas("min")),
        "single_value": AggregationSpecification("first"),
        # is null was checked earlier, now only need to compute the sum the non null values
        "regr_count": AggregationSpecification("sum", AggregationOnPandas("sum")),
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

        dc = DataContainer(df, cc)

        if not group_columns:
            # There was actually no GROUP BY specified in the SQL
            # Still, this plan can also be used if we need to aggregate something over the full
            # data sample
            # To reuse the code, we just create a new column at the end with a single value
            logger.debug("Performing full-table aggregation")

        # Do all aggregates
        df_result, output_column_order = self._do_aggregations(
            rel, dc, group_columns, context,
        )

        # SQL does not care about the index, but we do not want to have any multiindices
        df_agg = df_result.reset_index(drop=True)

        # Fix the column names and the order of them, as this was messed with during the aggregations
        df_agg.columns = df_agg.columns.get_level_values(-1)
        cc = ColumnContainer(df_agg.columns).limit_to(output_column_order)

        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        dc = DataContainer(df_agg, cc)
        dc = self.fix_dtype_to_row_type(dc, rel.getRowType())
        return dc

    def _do_aggregations(
        self,
        rel: "org.apache.calcite.rel.RelNode",
        dc: DataContainer,
        group_columns: List[str],
        context: "dask_sql.Context",
    ) -> Tuple[dd.DataFrame, List[str]]:
        """
        Main functionality: return the result dataframe
        and the output column order
        """
        df = dc.df
        cc = dc.column_container

        # We might need it later.
        # If not, lets hope that adding a single column should not
        # be a huge problem...
        additional_column_name = new_temporary_column(df)
        df = df.assign(**{additional_column_name: 1})

        # Add an entry for every grouped column, as SQL wants them first
        output_column_order = group_columns.copy()

        # Collect all aggregations we need to do
        collected_aggregations, output_column_order, df = self._collect_aggregations(
            rel, df, cc, context, additional_column_name, output_column_order
        )

        if not collected_aggregations:
            return df[group_columns].drop_duplicates(), output_column_order

        # SQL needs to have a column with the grouped values as the first
        # output column.
        # As the values of the group columns
        # are the same for a single group anyways, we just use the first row
        for col in group_columns:
            collected_aggregations[None].append((col, col, "first"))

        # Now we can go ahead and use these grouped aggregations
        # to perform the actual aggregation
        # It is very important to start with the non-filtered entry.
        # Otherwise we might loose some entries in the grouped columns
        df_result = None
        key = None
        if key in collected_aggregations:
            aggregations = collected_aggregations.pop(key)
            df_result = self._perform_aggregation(
                df, None, aggregations, additional_column_name, group_columns,
            )

        # Now we can also the the rest
        for filter_column, aggregations in collected_aggregations.items():
            agg_result = self._perform_aggregation(
                df, filter_column, aggregations, additional_column_name, group_columns,
            )

            # ... and finally concat the new data with the already present columns
            if df_result is None:
                df_result = agg_result
            else:
                df_result = df_result.assign(
                    **{col: agg_result[col] for col in agg_result.columns}
                )

        return df_result, output_column_order

    def _collect_aggregations(
        self,
        rel: "org.apache.calcite.rel.RelNode",
        df: dd.DataFrame,
        cc: ColumnContainer,
        context: "dask_sql.Context",
        additional_column_name: str,
        output_column_order: List[str],
    ) -> Tuple[
        Dict[Tuple[str, str], List[Tuple[str, str, Any]]], List[str], dd.DataFrame
    ]:
        """
        Collect all aggregations together, which have the same filter column
        so that the aggregations only need to be done once.

        Returns the aggregations as mapping filter_column -> List of Aggregations
        where the aggregations are in the form (input_col, output_col, aggregation function (or string))
        """
        collected_aggregations = defaultdict(list)

        for agg_call in rel.getNamedAggCalls():
            expr = agg_call.getKey()
            # Find out which aggregation function to use
            schema_name, aggregation_name = context.fqn(
                expr.getAggregation().getNameAsId()
            )
            aggregation_name = aggregation_name.lower()
            # Find out about the input column
            inputs = expr.getArgList()
            if aggregation_name == "regr_count":
                is_null = IsNullOperation()
                two_columns_proxy = new_temporary_column(df)
                if len(inputs) == 1:
                    # calcite some times gives one input/col to regr_count and
                    # another col has filter column
                    col1 = cc.get_backend_by_frontend_index(inputs[0])
                    df = df.assign(**{two_columns_proxy: (~is_null(df[col1]))})

                else:
                    col1 = cc.get_backend_by_frontend_index(inputs[0])
                    col2 = cc.get_backend_by_frontend_index(inputs[1])
                    # both cols should be not null
                    df = df.assign(
                        **{
                            two_columns_proxy: (
                                ~is_null(df[col1]) & (~is_null(df[col2]))
                            )
                        }
                    )
                input_col = two_columns_proxy
            elif len(inputs) == 1:
                input_col = cc.get_backend_by_frontend_index(inputs[0])
            elif len(inputs) == 0:
                input_col = additional_column_name
            else:
                raise NotImplementedError("Can not cope with more than one input")

            # Extract flags (filtering/distinct)
            if expr.isDistinct():  # pragma: no cover
                raise ValueError("Apache Calcite should optimize them away!")

            filter_column = None
            if expr.hasFilter():
                filter_column = cc.get_backend_by_frontend_index(expr.filterArg)

            try:
                aggregation_function = self.AGGREGATION_MAPPING[aggregation_name]
            except KeyError:
                try:
                    aggregation_function = context.schema[schema_name].functions[
                        aggregation_name
                    ]
                except KeyError:  # pragma: no cover
                    raise NotImplementedError(
                        f"Aggregation function {aggregation_name} not implemented (yet)."
                    )
            if isinstance(aggregation_function, AggregationSpecification):
                dtype = df[input_col].dtype
                if pd.api.types.is_numeric_dtype(dtype):
                    aggregation_function = aggregation_function.numerical_aggregation
                else:
                    aggregation_function = (
                        aggregation_function.non_numerical_aggregation
                    )

            # Finally, extract the output column name
            output_col = str(agg_call.getValue())

            # Store the aggregation
            key = filter_column
            value = (input_col, output_col, aggregation_function)
            collected_aggregations[key].append(value)
            output_column_order.append(output_col)

        return collected_aggregations, output_column_order, df

    def _perform_aggregation(
        self,
        df: dd.DataFrame,
        filter_column: str,
        aggregations: List[Tuple[str, str, Any]],
        additional_column_name: str,
        group_columns: List[str],
    ):
        tmp_df = df

        if filter_column:
            filter_expression = tmp_df[filter_column]
            tmp_df = tmp_df[filter_expression]

            logger.debug(f"Filtered by {filter_column} before aggregation.")

        group_columns = [tmp_df[group_column] for group_column in group_columns]
        group_columns_and_nulls = get_groupby_with_nulls_cols(
            tmp_df, group_columns, additional_column_name
        )
        grouped_df = tmp_df.groupby(by=group_columns_and_nulls)

        # Convert into the correct format for dask
        aggregations_dict = defaultdict(dict)
        for aggregation in aggregations:
            input_col, output_col, aggregation_f = aggregation

            aggregations_dict[input_col][output_col] = aggregation_f

        # Now apply the aggregation
        logger.debug(f"Performing aggregation {dict(aggregations_dict)}")
        agg_result = grouped_df.agg(aggregations_dict)

        # ... fix the column names to a single level ...
        agg_result.columns = agg_result.columns.get_level_values(-1)

        return agg_result
