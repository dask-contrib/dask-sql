import dask.dataframe as dd

from dask_sql.physical.rex.base import BaseRexPlugin


class RexInputRefPlugin(BaseRexPlugin):
    """
    A RexInputRef is an expression, which references a single column.
    It is typically to be found in any expressions which
    calculate a function in a column of a table.
    """

    class_name = "org.apache.calcite.rex.RexInputRef"

    def convert(
        self, rex: "org.apache.calcite.rex.RexNode", df: dd.DataFrame
    ) -> dd.Series:
        # The column is references by index
        index = rex.getIndex()
        return df.iloc[:, index]
