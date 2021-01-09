from functools import reduce
import operator
from typing import Tuple, List
import warnings
import logging

import numpy as np
import dask.dataframe as dd

from dask_sql.physical.rex import RexConverter
from dask_sql.java import org
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.rel.logical.filter import filter_or_scalar
from dask_sql.datacontainer import DataContainer, ColumnContainer

logger = logging.getLogger(__name__)


class LogicalJoinPlugin(BaseRelPlugin):
    """
    A LogicalJoin is used when (surprise) joining two tables.
    SQL allows for quite complicated joins with difficult conditions.
    dask/pandas only knows about equijoins on a specific column.

    We use a trick, which is also used in e.g. blazingSQL:
    we split the join condition into two parts:
    * everything which is an equijoin
    * the rest
    The first part is then used for the dask merging,
    whereas the second part is just applied as a filter afterwards.
    This will make joining more time-consuming that is needs to be
    but so far, it is the only solution...
    """

    class_name = "org.apache.calcite.rel.logical.LogicalJoin"

    JOIN_TYPE_MAPPING = {
        "INNER": "inner",
        "LEFT": "left",
        "RIGHT": "right",
        "FULL": "outer",
    }

    def convert(
        self, rel: "org.apache.calcite.rel.RelNode", context: "dask_sql.Context"
    ) -> DataContainer:
        # Joining is a bit more complicated, so lets do it in steps:

        # 1. We now have two inputs (from left and right), so we fetch them both
        dc_lhs, dc_rhs = self.assert_inputs(rel, 2, context)
        cc_lhs = dc_lhs.column_container
        cc_rhs = dc_rhs.column_container

        # 2. dask's merge will do some smart things with columns, which have the same name
        # on lhs an rhs (which also includes reordering).
        # However, that will confuse our column numbering in SQL.
        # So we make our life easier by converting the column names into unique names
        # We will convert back in the end
        cc_lhs_renamed = cc_lhs.make_unique("lhs")
        cc_rhs_renamed = cc_rhs.make_unique("rhs")

        dc_lhs_renamed = DataContainer(dc_lhs.df, cc_lhs_renamed)
        dc_rhs_renamed = DataContainer(dc_rhs.df, cc_rhs_renamed)

        df_lhs_renamed = dc_lhs_renamed.assign()
        df_rhs_renamed = dc_rhs_renamed.assign()

        join_type = rel.getJoinType()
        join_type = self.JOIN_TYPE_MAPPING[str(join_type)]

        # 3. The join condition can have two forms, that we can understand
        # (a) a = b
        # (b) X AND Y AND a = b AND Z ... (can also be multiple a = b)
        # The first case is very simple and we do not need any additional filter
        # In the second case we do a merge on all the a = b,
        # and then apply a filter using the other expressions.
        # In all other cases, we need to do a full table cross join and filter afterwards.
        # As this is probably non-sense for large tables, but there is no other
        # known solution so far.
        join_condition = rel.getCondition()
        lhs_on, rhs_on, filter_condition = self._split_join_condition(join_condition)

        logger.debug(f"Joining with type {join_type} on columns {lhs_on}, {rhs_on}.")

        # lhs_on and rhs_on are the indices of the columns to merge on.
        # The given column indices are for the full, merged table which consists
        # of lhs and rhs put side-by-side (in this order)
        # We therefore need to normalize the rhs indices relative to the rhs table.
        rhs_on = [index - len(df_lhs_renamed.columns) for index in rhs_on]

        # 4. dask can only merge on the same column names.
        # We therefore create new columns on purpose, which have a distinct name.
        assert len(lhs_on) == len(rhs_on)
        if lhs_on:
            lhs_columns_to_add = {
                f"common_{i}": df_lhs_renamed.iloc[:, index]
                for i, index in enumerate(lhs_on)
            }
            rhs_columns_to_add = {
                f"common_{i}": df_rhs_renamed.iloc[:, index]
                for i, index in enumerate(rhs_on)
            }
        else:
            # We are in the complex join case
            # where we have no column to merge on
            # This means we have no other chance than to merge
            # everything with everything...
            # We add a 1-column to merge on
            lhs_columns_to_add = {"common": 1}
            rhs_columns_to_add = {"common": 1}

            warnings.warn(
                "Need to do a cross-join, which is typically very resource heavy",
                ResourceWarning,
            )

        df_lhs_with_tmp = df_lhs_renamed.assign(**lhs_columns_to_add)
        df_rhs_with_tmp = df_rhs_renamed.assign(**rhs_columns_to_add)
        added_columns = list(lhs_columns_to_add.keys())

        # 5. Now we can finally merge on these columns
        # The resulting dataframe will contain all (renamed) columns from the lhs and rhs
        # plus the added columns
        df = dd.merge(df_lhs_with_tmp, df_rhs_with_tmp, on=added_columns, how=join_type)

        # 6. So the next step is to make sure
        # we have the correct column order (and to remove the temporary join columns)
        correct_column_order = list(df_lhs_renamed.columns) + list(
            df_rhs_renamed.columns
        )
        cc = ColumnContainer(df.columns).limit_to(correct_column_order)

        # and to rename them like the rel specifies
        row_type = rel.getRowType()
        field_specifications = [str(f) for f in row_type.getFieldNames()]
        cc = cc.rename(
            {
                from_col: to_col
                for from_col, to_col in zip(cc.columns, field_specifications)
            }
        )
        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        dc = DataContainer(df, cc)

        # 7. Last but not least we apply any filters by and-chaining together the filters
        if filter_condition:
            # This line is a bit of code duplication with RexCallPlugin - but I guess it is worth to keep it separate
            filter_condition = reduce(
                operator.and_,
                [
                    RexConverter.convert(rex, dc, context=context)
                    for rex in filter_condition
                ],
            )
            logger.debug(f"Additionally applying filter {filter_condition}")
            df = filter_or_scalar(df, filter_condition)
            dc = DataContainer(df, cc)

        dc = self.fix_dtype_to_row_type(dc, rel.getRowType())
        return dc

    def _split_join_condition(
        self, join_condition: "org.apache.calcite.rex.RexCall"
    ) -> Tuple[List[str], List[str], List["org.apache.calcite.rex.RexCall"]]:

        if isinstance(join_condition, org.apache.calcite.rex.RexLiteral):
            return [], [], [join_condition]
        elif not isinstance(join_condition, org.apache.calcite.rex.RexCall):
            raise NotImplementedError("Can not understand join condition.")

        # Simplest case: ... ON lhs.a == rhs.b
        try:
            lhs_on, rhs_on = self._extract_lhs_rhs(join_condition)
            return [lhs_on], [rhs_on], None
        except AssertionError:
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
                except AssertionError:
                    pass

                filter_condition.append(operand)

            if lhs_on and rhs_on:
                return lhs_on, rhs_on, filter_condition

        return [], [], [join_condition]

    def _extract_lhs_rhs(self, rex):
        operator_name = str(rex.getOperator().getName())
        assert operator_name == "="

        operands = rex.getOperands()
        assert len(operands) == 2

        operand_lhs = operands[0]
        operand_rhs = operands[1]

        if isinstance(operand_lhs, org.apache.calcite.rex.RexInputRef) and isinstance(
            operand_rhs, org.apache.calcite.rex.RexInputRef
        ):
            lhs_index = operand_lhs.getIndex()
            rhs_index = operand_rhs.getIndex()

            # The rhs table always comes after the lhs
            # table. Therefore we have a very simple
            # way of checking, which index comes from which
            # input
            if lhs_index > rhs_index:
                lhs_index, rhs_index = rhs_index, lhs_index

            return lhs_index, rhs_index

        raise TypeError(
            "Invalid join condition"
        )  # pragma: no cover. Do not how how it could be triggered.
