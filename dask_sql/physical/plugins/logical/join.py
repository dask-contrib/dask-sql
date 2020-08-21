from functools import reduce
import operator

import dask.dataframe as dd

from dask_sql.physical.ral import convert_ral_to_df
from dask_sql.physical.rex import apply_rex_call
from dask_sql.java import get_short_java_class, List


class LogicalJoinPlugin:
    class_name = "org.apache.calcite.rel.logical.LogicalJoin"

    JOIN_TYPE_MAPPING ={
        "INNER": "inner",
        "LEFT": "left",
        "RIGHT": "right",
        "FULL": "outer",
    }

    def __call__(self, ral, tables):
        # Joining is a bit more complicated, so lets do it in steps:

        # 1. We now have two inputs (from left and right), so we fetch them both
        input_rals = ral.getInputs()
        assert len(input_rals) == 2
        df_lhs = convert_ral_to_df(input_rals[0], tables)
        df_rhs = convert_ral_to_df(input_rals[1], tables)

        # 2. dask's merge will do some smart things with columns, which are the same on lhs an rhs.
        # However, that will confuse our column numbering in SQL.
        # So we make our life easier by converting the column names into unique names
        # We will convert back in the end
        df_lhs_renamed = df_lhs.rename(columns={col: f"lhs_{i}" for i, col in enumerate(df_lhs.columns)})
        df_rhs_renamed = df_rhs.rename(columns={col: f"rhs_{i}" for i, col in enumerate(df_rhs.columns)})

        join_type = ral.getJoinType()
        join_type = self.JOIN_TYPE_MAPPING[str(join_type)]

        # 3. The join condition can have two forms, that we can understand
        # (a) a = b
        # (b) X AND Y AND a = b AND Z ... (can also be multiple a = b)
        # The first case is very simple and we do not need any additional filter
        # The second case we do a merge on all the a = b, and then apply a filter using the other expressions
        # In all other cases, we would need to do a full table cross join and filter afterwards.
        # As this is probably non-sense for most of the cases, we just disallow it for now
        join_condition = ral.getCondition()
        lhs_on, rhs_on, filter_condition = self._split_join_condition(join_condition)

        # lhs_on and rhs_on are the indices of the columns to merge on.
        # The given column indices are for the full, merged table which consists
        # of lhs and rhs put side-by-side (in this order)
        # We therefore need to normalize the rhs indices relative to the rhs table.
        rhs_on = [index - len(df_lhs.columns) for index in rhs_on]

        # 4. dask can only merge on the same column names.
        # We therefore create new columns on purpose, which have a distinct name.
        assert len(lhs_on) == len(rhs_on)
        lhs_columns_to_add = {f"common_{i}": df_lhs_renamed.iloc[:, index] for i, index in enumerate(lhs_on)}
        rhs_columns_to_add = {f"common_{i}": df_rhs_renamed.iloc[:, index] for i, index in enumerate(rhs_on)}

        df_lhs_with_tmp = df_lhs_renamed.assign(**lhs_columns_to_add)
        df_rhs_with_tmp = df_rhs_renamed.assign(**rhs_columns_to_add)
        added_columns = list(lhs_columns_to_add.keys())

        # 5. Now we can finally merge on these columns
        # The resulting dataframe will contain all (renamed) columns from the lhs and rhs
        # plus the added columns
        df = dd.merge(df_lhs_with_tmp, df_rhs_with_tmp, on=added_columns)

        # 6. So the next step is to make sure
        # we have the correct column order (and to remove the temporary join columns)
        df = df[list(df_lhs_renamed.columns) + list(df_rhs_renamed.columns)]

        # Now we also go back to the original names (just to have the output nicer)
        df = df.rename(columns={f"lhs_{i}": col for i, col in enumerate(df_lhs.columns)})
        df = df.rename(columns={f"rhs_{i}": col for i, col in enumerate(df_rhs.columns)})

        # 7. Last but not least we apply any filters by and-chaining together the filters
        if filter_condition:
            # This line is a bit of code duplication with RexCallPlugin - but I guess it is worth to keep it separate
            filter_condition = reduce(operator.and_, [apply_rex_call(rex, df) for rex in filter_condition])
            df = df[filter_condition]

        return df

    def _split_join_condition(self, join_condition):
        assert get_short_java_class(join_condition) == "RexCall"

        # Simplest case: ... ON lhs.a == rhs.b
        try:
            lhs_on, rhs_on = self._extract_lhs_rhs(join_condition)
            return [lhs_on], [rhs_on], None
        except (TypeError, AssertionError):
            pass

        operator_name = str(join_condition.getOperator().getName())
        operands = join_condition.getOperands()
        # More complicated: ... ON X AND Y AND Z.
        # We can map this if one of them is again a "="
        if operator_name == "AND":
            lhs_on = []
            rhs_on = []
            filter_condition = []

            for operand in operands:
                try:
                    lhs_on_part, rhs_on_part = self._extract_lhs_rhs(operand)
                    lhs_on.append(lhs_on_part)
                    rhs_on.append(rhs_on_part)
                    continue
                except (TypeError, AssertionError):
                    pass

                filter_condition.append(operand)

            if lhs_on and rhs_on:
                return lhs_on, rhs_on, filter_condition

        raise NotImplementedError("The join condition is too complex")

    def _extract_lhs_rhs(self, rex):
        operator_name = str(rex.getOperator().getName())
        assert operator_name == "="

        operands = rex.getOperands()
        assert len(operands) == 2

        operand_lhs = operands[0]
        operand_rhs = operands[1]

        if get_short_java_class(operand_lhs) == "RexInputRef" and get_short_java_class(operand_rhs) == "RexInputRef":
            lhs_index = operand_lhs.getIndex()
            rhs_index = operand_rhs.getIndex()

            return lhs_index, rhs_index

        raise TypeError("Can not extract lhs and rhs from this node") # pragma: no cover