import logging
from typing import TYPE_CHECKING

import dask_sql.utils as utils
from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_sql._datafusion_lib import LogicalPlan

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
        cc_lhs = dc_lhs.column_container
        cc_rhs = dc_rhs.column_container

        # Rename the columns of the left and right inputs
        df_lhs_renamed = DataContainer(dc_lhs.df, cc_lhs.make_unique("lhs")).assign()
        df_rhs_renamed = DataContainer(dc_rhs.df, cc_rhs.make_unique("rhs")).assign()

        # Create a 'key' column in both DataFrames to join on
        cross_join_key = utils.new_temporary_column(df_lhs_renamed)
        df_lhs_renamed[cross_join_key] = 1
        df_rhs_renamed[cross_join_key] = 1

        result = df_lhs_renamed.merge(
            df_rhs_renamed, on=cross_join_key, suffixes=("", "0")
        ).drop(cross_join_key, 1)
        cc = ColumnContainer(result.columns)

        # Rename columns like the rel specifies
        row_type = rel.getRowType()
        field_specifications = [str(f) for f in row_type.getFieldNames()]

        cc = cc.rename(
            {
                from_col: to_col
                for from_col, to_col in zip(cc.columns, field_specifications)
            }
        )
        cc = self.fix_column_to_row_type(cc, row_type)
        return DataContainer(result, cc)
