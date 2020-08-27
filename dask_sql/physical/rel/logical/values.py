from typing import Dict

import dask.dataframe as dd
import pandas as pd

from dask_sql.physical.rex import RexConverter
from dask_sql.physical.rel.base import BaseRelPlugin


class LogicalValuesPlugin(BaseRelPlugin):
    """
    A LogicalValue is a table just consisting of
    raw values (nothing database-dependent).
    For example

        SELECT 1 + 1;

    We generate a pandas dataframe and a dask
    dataframe out of it directly here.
    We assume that this will only ever be used for small
    data samples.
    """

    class_name = "org.apache.calcite.rel.logical.LogicalValues"

    def convert(
        self, rel: "org.apache.calcite.rel.RelNode", tables: Dict[str, dd.DataFrame]
    ) -> dd.DataFrame:
        # There should not be any input. This is the first step.
        self.assert_inputs(rel, 0)

        rex_expression_rows = list(rel.getTuples())
        rows = []
        for rex_expressions in rex_expression_rows:
            rows.append([RexConverter.convert(rex, None) for rex in rex_expressions])

        # We assume here that when using the values plan, the resulting dataframe will be quite small
        # TODO: we explicitely reference pandas and dask here -> might we worth making this more general
        df = dd.from_pandas(pd.DataFrame(rows), npartitions=1)
        df = self.fix_column_to_row_type(df, rel.getRowType())

        return df
