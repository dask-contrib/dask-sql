from collections import defaultdict
from typing import Dict, List, Tuple

import dask.dataframe as dd

from dask_sql.physical.rel.base import BaseRelPlugin


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
        "$SUM0": "sum",
        "COUNT": "count",
        "MAX": "max",
        "MIN": "min",
        "SINGLE_VALUE": "first",
    }

    def convert(
        self, rel: "org.apache.calcite.rel.RelNode", tables: Dict[str, dd.DataFrame]
    ) -> dd.DataFrame:
        (df,) = self.assert_inputs(rel, 1, tables)

        # We make our life easier with having unique column names
        df = self.make_unique(df)

        # I have no idea what that is, but so far it was always of length 1
        assert len(rel.getGroupSets()) == 1, "Do not know how to handle this case!"

        # Extract the information, which columns we need to group for
        group_column_indices = [int(i) for i in rel.getGroupSet()]
        group_columns = [df.columns[i] for i in group_column_indices]

        # Always keep an additional column around for empty groups and aggregates
        additional_column_name = str(len(df.columns))
        df = df.assign(**{additional_column_name: 1})

        # Collect all aggregates
        aggregations, output_column_order = self._collect_aggregations(
            rel, df, group_columns, additional_column_name
        )

        if not group_columns:
            # There was actually no GROUP BY specified in the SQL
            # Still, this plan can also be used if we need to aggregate something over the full
            # data sample
            # To reuse the code, we just create a new column at the end with a single value
            # It is important to do this after creating the aggregations,
            # as we do not want this additional column to be used anywhere
            group_columns = [additional_column_name]

        # Now we can perform the aggregates
        df = df.groupby(by=group_columns).agg(aggregations)

        # SQL does not care about the index, but we do not want to have any multiindices
        df = df.reset_index(drop=True)

        # Fix the column names and the order of them, as this was messed with during the aggregations
        df.columns = df.columns.get_level_values(-1)
        df = df[output_column_order]

        df = self.fix_column_to_row_type(df, rel.getRowType())

        return df

    def _collect_aggregations(
        self,
        rel: "org.apache.calcite.rel.RelNode",
        df: dd.DataFrame,
        group_columns: List[str],
        additional_column_name: str,
    ) -> Tuple[Dict[str, Dict[str, str]], List[int]]:
        aggregations = defaultdict(dict)
        output_column_order = []

        # SQL needs to copy the old content also. As the values are the same for a single group
        # anyways, we just use the first row
        for col in group_columns:
            aggregations[col][col] = "first"
            output_column_order.append(col)

        for agg_call in rel.getNamedAggCalls():
            output_column_name = str(agg_call.getValue())
            expr = agg_call.getKey()

            if expr.isDistinct():
                raise NotImplementedError(
                    "DISTINCT is not implemented (yet)"
                )  # pragma: no cover

            aggregation_name = str(expr.getAggregation().getName())
            try:
                aggregation_function = self.AGGREGATION_MAPPING[aggregation_name]
            except KeyError:  # pragma: no cover
                raise NotImplementedError(
                    f"Aggregation function {aggregation_name} not implemented (yet)."
                )

            inputs = expr.getArgList()
            if len(inputs) == 1:
                input_column_name = df.columns[inputs[0]]
            elif len(inputs) == 0:
                input_column_name = additional_column_name
            else:
                raise NotImplementedError(
                    "Can not cope with more than one input"
                )  # pragma: no cover

            aggregations[input_column_name][output_column_name] = aggregation_function
            output_column_order.append(output_column_name)

        return aggregations, output_column_order
