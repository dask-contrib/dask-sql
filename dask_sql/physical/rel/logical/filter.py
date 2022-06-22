import logging
from typing import TYPE_CHECKING, Union

import dask.config as dask_config
import dask.dataframe as dd
import numpy as np

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.rex import RexConverter
from dask_sql.physical.utils.filter import attempt_predicate_pushdown

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan

logger = logging.getLogger(__name__)


def filter_or_scalar(df: dd.DataFrame, filter_condition: Union[np.bool_, dd.Series]):
    """
    Some (complex) SQL queries can lead to a strange condition which is always true or false.
    We do not need to filter in this case.
    See https://github.com/dask-contrib/dask-sql/issues/87.
    """
    if np.isscalar(filter_condition):
        if not filter_condition:  # pragma: no cover
            # empty dataset
            logger.warning("Join condition is always false - returning empty dataset")
            return df.head(0, compute=False)
        else:
            return df

    # In SQL, a NULL in a boolean is False on filtering
    filter_condition = filter_condition.fillna(False)
    out = df[filter_condition]
    if dask_config.get("sql.predicate_pushdown"):
        return attempt_predicate_pushdown(out)
    else:
        return out


class DaskFilterPlugin(BaseRelPlugin):
    """
    DaskFilter is used on WHERE clauses.
    We just evaluate the filter (which is of type RexNode) and apply it
    """

    class_name = "Filter"

    def convert(
        self,
        rel: "LogicalPlan",
        context: "dask_sql.Context",
    ) -> DataContainer:
        (dc,) = self.assert_inputs(rel, 1, context)
        df = dc.df
        cc = dc.column_container

        filter = rel.filter()

        # Every logic is handled in the RexConverter
        # we just need to apply it here
        condition = filter.getCondition()
        df_condition = RexConverter.convert(rel, condition, dc, context=context)
        df = filter_or_scalar(df, df_condition)

        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        return DataContainer(df, cc)
