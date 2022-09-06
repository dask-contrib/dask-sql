import logging
from typing import TYPE_CHECKING

import dask.dataframe as dd

import dask_sql.utils as utils
from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan

logger = logging.getLogger(__name__)


class DaskCrossJoinPlugin(BaseRelPlugin):
    """
    While similar to `DaskJoinPlugin` a `CrossJoin` has enough of a differing
    structure to justify its own plugin. This in turn limits the number of
    Dask tasks that are generated for `CrossJoin`'s when compared to a
    standard `Join`
    """

    class_name = "CrossJoin"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        # We now have two inputs (from left and right), so we fetch them both
        dc_lhs, dc_rhs = self.assert_inputs(rel, 2, context)

        df_lhs = dc_lhs.df
        df_rhs = dc_rhs.df

        # Create a 'key' column in both DataFrames to join on
        cross_join_key = utils.new_temporary_column(df_lhs)
        df_lhs[cross_join_key] = 1
        df_rhs[cross_join_key] = 1

        result = dd.merge(df_lhs, df_rhs, on=cross_join_key, suffixes=("", "0")).drop(
            cross_join_key, 1
        )

        return DataContainer(result, ColumnContainer(result.columns))
