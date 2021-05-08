from typing import Tuple

import dask.dataframe as dd

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rex.base import BaseRexPlugin, ColumnReference, OutputColumn


class RexInputRefPlugin(BaseRexPlugin):
    """
    A RexInputRef is an expression, which references a single column.
    It is typically to be found in any expressions which
    calculate a function in a column of a table.

    Important: the returned column reference references
    the real backend column in the dataframe, not
    in the frontend.
    """

    class_name = "org.apache.calcite.rex.RexInputRef"

    def convert(
        self,
        rex: "org.apache.calcite.rex.RexNode",
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> Tuple[OutputColumn, DataContainer]:
        cc = dc.column_container

        # The column is references by index
        index = rex.getIndex()
        backend_column_name = cc.get_backend_by_frontend_index(index)
        return ColumnReference(backend_column_name), dc
