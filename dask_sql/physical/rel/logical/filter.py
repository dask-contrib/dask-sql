from typing import Dict

import dask.dataframe as dd

from dask_sql.physical.rex import RexConverter
from dask_sql.physical.rel.base import BaseRelPlugin


class LogicalFilterPlugin(BaseRelPlugin):
    """
    LogicalFilter is used on WHERE clauses.
    We just evaluate the filter (which is of type RexNode) and apply it
    """

    class_name = "org.apache.calcite.rel.logical.LogicalFilter"

    def convert(
        self, rel: "org.apache.calcite.rel.RelNode", context: "dask_sql.Context"
    ) -> dd.DataFrame:
        (df,) = self.assert_inputs(rel, 1, context)
        self.check_columns_from_row_type(df, rel.getExpectedInputRowType(0))

        condition = rel.getCondition()
        df_condition = RexConverter.convert(condition, df, context=context)
        df = df[df_condition]

        df = self.fix_column_to_row_type(df, rel.getRowType())
        return df
