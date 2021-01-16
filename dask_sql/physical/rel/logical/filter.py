from typing import Union
import logging

import dask.dataframe as dd
import numpy as np

from dask_sql.physical.rex import RexConverter
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import DataContainer


logger = logging.getLogger(__name__)


def filter_or_scalar(df: dd.DataFrame, filter_condition: Union[np.bool_, dd.Series]):
    """
    Some (complex) SQL queries can lead to a strange condition which is always true or false.
    We do not need to filter in this case.
    See https://github.com/nils-braun/dask-sql/issues/87.
    """
    if np.isscalar(filter_condition):
        if not filter_condition:
            # empty dataset
            logger.warning("Join condition is always false - returning empty dataset")
            return df.head(0, compute=False)
        else:
            return df

    return df[filter_condition]


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
        df = filter_or_scalar(df, df_condition)

        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        # No column type has changed, so no need to convert again
        return DataContainer(df, cc)
