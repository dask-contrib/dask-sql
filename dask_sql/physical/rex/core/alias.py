from typing import TYPE_CHECKING, Any, Union

import dask.dataframe as dd

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rex import RexConverter
from dask_sql.physical.rex.base import BaseRexPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import Expression, LogicalPlan


class RexAliasPlugin(BaseRexPlugin):
    """
    A RexAliasPlugin is an expression, which references a Subquery.
    This plugin is thin on logic, however keeping with previous patterns
    we use the plugin approach instead of placing the logic inline
    """

    class_name = "RexAlias"

    def convert(
        self,
        rel: "LogicalPlan",
        rex: "Expression",
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> Union[dd.Series, Any]:
        # extract the operands; there should only be a single underlying Expression
        operands = rex.getOperands()
        assert len(operands) == 1

        sub_rex = operands[0]

        value = RexConverter.convert(rel, sub_rex, dc, context=context)

        if isinstance(value, DataContainer):
            return value.df

        return value
