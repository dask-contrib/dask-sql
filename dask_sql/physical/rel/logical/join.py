import logging
import operator
import warnings
from functools import reduce
from typing import List, Tuple

import dask.dataframe as dd
# Need pd.NA
import pandas as pd
from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.java import org
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.rel.logical.filter import filter_or_scalar
from dask_sql.physical.rex import RexConverter

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
        # Add two columns (1,2,...) to keep track of observations in left and
        # right tables. They must be at the end of the columns since
        # _join_on_columns needs the relative order of columns (lhs_on and rhs_on)
        # Only dask-supported functions are used (assign and cumsum) so that a
        # compute() is not triggered.
        df_lhs_renamed = df_lhs_renamed.assign(left_idx=1)
        df_lhs_renamed = df_lhs_renamed.assign(
            left_idx=df_lhs_renamed["left_idx"].cumsum()
        )
        df_rhs_renamed = df_rhs_renamed.assign(right_idx=1)
        df_rhs_renamed = df_rhs_renamed.assign(
            right_idx=df_rhs_renamed["right_idx"].cumsum()
        )

        if lhs_on:
            # 5. Now we can finally merge on these columns
            # The resulting dataframe will contain all (renamed) columns from the lhs and rhs
            # plus the added columns
            # Need the indicator for left/right join
            df = self._join_on_columns(
                df_lhs_renamed, 
                df_rhs_renamed,
                lhs_on,
                rhs_on,
                join_type, 
                indicator=True,
            )
        else:
            # 5. We are in the complex join case
            # where we have no column to merge on
            # This means we have no other chance than to merge
            # everything with everything...

            # TODO: we should implement a shortcut
            # for filter conditions that are always false

            def merge_single_partitions(lhs_partition, rhs_partition):
                # Do a cross join with the two partitions
                # TODO: it would be nice to apply the filter already here
                # problem: this would mean we need to ship the rex to the
                # workers (as this is executed on the workers),
                # which is definitely not possible (java dependency, JVM start...)
                lhs_partition = lhs_partition.assign(common=1)
                rhs_partition = rhs_partition.assign(common=1)
                merged_data = lhs_partition.merge(rhs_partition, on=["common"])

                return merged_data

            # Iterate nested over all partitions from lhs and rhs and merge them
            name = "cross-join-" + tokenize(df_lhs_renamed, df_rhs_renamed)
            dsk = {
                (name, i * df_rhs_renamed.npartitions + j): (
                    merge_single_partitions,
                    (df_lhs_renamed._name, i),
                    (df_rhs_renamed._name, j),
                )
                for i in range(df_lhs_renamed.npartitions)
                for j in range(df_rhs_renamed.npartitions)
            }

            graph = HighLevelGraph.from_collections(
                name, dsk, dependencies=[df_lhs_renamed, df_rhs_renamed]
            )

            meta = dd.dispatch.concat(
                [df_lhs_renamed._meta_nonempty, df_rhs_renamed._meta_nonempty], axis=1
            )
            # TODO: Do we know the divisions in any way here?
            divisions = [None] * (len(dsk) + 1)
            df = dd.DataFrame(graph, name, meta=meta, divisions=divisions)

            warnings.warn(
                "Need to do a cross-join, which is typically very resource heavy",
                ResourceWarning,
            )

            # Add _merge to be consistent with the case lhs_on=True
            df["_merge"] = "both"
            df["_merge"] = df["_merge"].astype("category")
        # Put newly added columns to the end
        df = df[
            df.columns.drop("left_idx").insert(
                df.columns.get_loc("right_idx") - 1, "left_idx"
            )
        ]

        # Completely reset index to uniquely identify each row since there
        # could be duplicates. (Yeah. It may be better to inform users that
        # index will break. After all, it is expected to be broken since the
        # number of rows changes.
        df = df.assign(uniqid=1)
        df = df.assign(uniqid=df["uniqid"].cumsum()).set_index("uniqid")

        # 6. So the next step is to make sure
        # we have the correct column order (and to remove the temporary join columns)
        # Need to exclude temporary columns left_idx and right_idx
        correct_column_order = list(df_lhs_renamed.columns.drop("left_idx")) + list(
            df_rhs_renamed.columns.drop("right_idx")
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
        cc = self.fix_column_to_row_type(cc, row_type)
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
            # Three cases to deal with inequality conditions (left join as an example):
            # Case 1 [eq_unmatched] (Not matched by equality):
            #     Left-only from equality join (_merge=='left_only')
            #     => Keep all
            # Case 2 [ineq_unmatched] (Not matched by inequality):
            #     For unique left_idx, there are no True in filter_condition
            #     => Set values from right/left table to missing (NaN or NaT)
            #     => Keep 1 copy and drop duplicates over left_idx (there could
            #         be duplicates now due to equality match).
            # Case 3 (Matched by inequality):
            #     For unique left_idx, there are 1 or more True in filter_condition
            #     => Keep obs with True in filter_condition
            # This has to be added to df since partition will break the groupby
            df["filter_condition"] = filter_condition
            if join_type in ["left", "right"]:
                # ----- Case 1 (Not matched by equality)
                if join_type == "left":
                    # Flag obs unmatched in equality join
                    df["eq_unmatched"] = df["_merge"] == "left_only"
                    idx_varname = "left_idx"
                    other_varpre = "rhs_"
                else:
                    # Flag obs unmatched in equality join
                    df["eq_unmatched"] = df["_merge"] == "right_only"
                    idx_varname = "right_idx"
                    other_varpre = "lhs_"

                # ----- Case 2 (Not matched by inequality)

                # Set NA (pd.NA)
                # Flag obs not matched by inequality
                df = df.merge(
                    (df.groupby(idx_varname)["filter_condition"].agg("sum") < 1)
                    .rename("ineq_unmatched")
                    .to_frame(),
                    left_on=idx_varname,
                    right_index=True,
                    how="left",
                )
                # Assign pd.NA
                for v in df.columns[df.columns.str.startswith(other_varpre)]:
                    df[v] = df[v].mask(
                        df["ineq_unmatched"] & (~df["eq_unmatched"]), pd.NA
                    )

                # Drop duplicates
                # Flag the first obs for each unique left_idx
                # (or right_idx for right join) in order to remove duplicates
                df = df.merge(
                    df[[idx_varname]]
                    .drop_duplicates()
                    .assign(first_elem=True)
                    .drop(columns=[idx_varname]),
                    left_index=True,
                    right_index=True,
                    how="left",
                )
                df["first_elem"] = df["first_elem"].fillna(False)

                # ----- The full condition to keep observations
                filter_condition_all = (
                    df["filter_condition"]
                    | df["eq_unmatched"]
                    | (df["ineq_unmatched"] & df["first_elem"])
                )
                # Drop added temporary columns
                df = df.drop(
                    columns=[
                        "left_idx", 
                        "right_idx",
                        "_merge",
                        "filter_condition",
                        "eq_unmatched",
                        "ineq_unmatched",
                        "first_elem",
                    ]
                )
            elif join_type == "inner":
                filter_condition_all = filter_condition
                # TODO: Full Join

            logger.debug(f"Additionally applying filter {filter_condition_all}")
            df = filter_or_scalar(df, filter_condition_all)
            # Reset index (maybe notify users that dask-sql may break index)
            df = df.reset_index(drop=True)
            dc = DataContainer(df, cc)

        dc = self.fix_dtype_to_row_type(dc, rel.getRowType())
        return dc

    def _join_on_columns(
        self,
        df_lhs_renamed: dd.DataFrame,
        df_rhs_renamed: dd.DataFrame,
        lhs_on: List[str],
        rhs_on: List[str],
        join_type: str,
        indicator: bool = False,
    ) -> dd.DataFrame:
        lhs_columns_to_add = {
            f"common_{i}": df_lhs_renamed.iloc[:, index]
            for i, index in enumerate(lhs_on)
        }
        rhs_columns_to_add = {
            f"common_{i}": df_rhs_renamed.iloc[:, index]
            for i, index in enumerate(rhs_on)
        }

        # SQL compatibility: when joining on columns that
        # contain NULLs, pandas will actually happily
        # keep those NULLs. That is however not compatible with
        # SQL, so we get rid of them here
        if join_type in ["inner", "right"]:
            df_lhs_filter = reduce(
                operator.and_,
                [~df_lhs_renamed.iloc[:, index].isna() for index in lhs_on],
            )
            df_lhs_renamed = df_lhs_renamed[df_lhs_filter]
        if join_type in ["inner", "left"]:
            df_rhs_filter = reduce(
                operator.and_,
                [~df_rhs_renamed.iloc[:, index].isna() for index in rhs_on],
            )
            df_rhs_renamed = df_rhs_renamed[df_rhs_filter]

        df_lhs_with_tmp = df_lhs_renamed.assign(**lhs_columns_to_add)
        df_rhs_with_tmp = df_rhs_renamed.assign(**rhs_columns_to_add)
        added_columns = list(lhs_columns_to_add.keys())

        df = dd.merge(
            df_lhs_with_tmp,
            df_rhs_with_tmp,
            on=added_columns,
            how=join_type,
            indicator=indicator,
        )

        return df

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

        raise AssertionError(
            "Invalid join condition"
        )  # pragma: no cover. Do not how how it could be triggered.
