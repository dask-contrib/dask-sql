from collections import defaultdict

from dask_sql.physical.ral import convert_ral_to_df, fix_column_to_row_type


class LogicalAggregatePlugin:
    class_name = "org.apache.calcite.rel.logical.LogicalAggregate"

    AGGREGATION_MAPPING = {
        "$SUM0": "sum",
        "COUNT": "count",
        "MAX": "max",
        "MIN": "min",
        "SINGLE_VALUE": "first",
    }

    def __call__(self, ral, tables):
        assert len(ral.getInputs()) == 1

        input_ral = ral.getInput()
        df = convert_ral_to_df(input_ral, tables)

        # We make our life easier with having unique column names
        new_columns = dict(zip(df.columns, range(len(df.columns))))
        df = df.rename(columns=new_columns)

        # Extract the information, which columns we need to group for
        group_column_indices = [int(i) for i in ral.getGroupSet()]
        group_columns = [df.columns[i] for i in group_column_indices]

        # I have no idea what that is, but so far it was always of length 1
        assert len(ral.getGroupSets()) == 1, "Do not know how to handle this case!"

        aggregations = defaultdict(dict)
        output_column_order = []

        # SQL needs to copy the old content also. As the values are the same for a single group
        # anyways, we just use the first row
        for col in group_columns:
            aggregations[col][col] = "first"
            output_column_order.append(col)

        # Always keep an additional column around for empty groups and aggregates
        additional_column_name = str(len(df.columns))
        df = df.assign(**{additional_column_name: 1})

        for agg_call in ral.getNamedAggCalls():
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
                raise NotImplementedError("Can not cope with more than one input")

            aggregations[input_column_name][output_column_name] = aggregation_function
            output_column_order.append(output_column_name)

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

        df = fix_column_to_row_type(df, ral.getRowType())

        return df
