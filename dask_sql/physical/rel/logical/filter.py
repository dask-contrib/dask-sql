import logging
from typing import TYPE_CHECKING, Union

import dask.dataframe as dd
import numpy as np

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.rex import RexConverter

if TYPE_CHECKING:
    import dask_sql
    from dask_sql.java import org

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
    return df[filter_condition]


class DaskFilterPlugin(BaseRelPlugin):
    """
    DaskFilter is used on WHERE clauses.
    We just evaluate the filter (which is of type RexNode) and apply it
    """

    class_name = "com.dask.sql.nodes.DaskFilter"

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
        df = filter_or_scalar(df, df_condition)

        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        # No column type has changed, so no need to convert again
        return DataContainer(df, cc)
