from typing import Dict

import dask.dataframe as dd

from dask_sql.physical.rex import RexConverter
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import DataContainer


class LogicalFilterPlugin(BaseRelPlugin):
    """
    LogicalFilter is used on WHERE clauses.
    We just evaluate the filter (which is of type RexNode) and apply it
    """

    class_name = "org.apache.calcite.rel.logical.LogicalFilter"

    def convert(
        self, rel: "org.apache.calcite.rel.RelNode", context: "dask_sql.Context"
    ) -> DataContainer:
        (dc,) = self.assert_inputs(rel, 1, context)
        df = dc.df
        cc = dc.column_container

        # Every logic is handled in the RexConverter
        # we just need to apply it here
        condition = rel.getCondition()
        df_condition = RexConverter.convert(condition, dc, context=context)
        df = df[df_condition]

        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        # No column type has changed, so no need to convert again
        return DataContainer(df, cc)
