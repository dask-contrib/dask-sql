import dask.dataframe as dd
import pandas as pd

from dask_sql.physical.rex import RexConverter
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import DataContainer, ColumnContainer


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
        self, rel: "org.apache.calcite.rel.RelNode", context: "dask_sql.Context"
    ) -> DataContainer:
        # There should not be any input. This is the first step.
        self.assert_inputs(rel, 0)

        rex_expression_rows = list(rel.getTuples())
        rows = []
        for rex_expression_row in rex_expression_rows:
            # We convert each of the cells in the row
            # using a RexConverter.
            # As we do not have any information on the
            # column headers, we just name them with
            # their index.
            rows.append(
                {
                    str(i): RexConverter.convert(rex_cell, None, context=context)
                    for i, rex_cell in enumerate(rex_expression_row)
                }
            )

        # TODO: we explicitely reference pandas and dask here -> might we worth making this more general
        # We assume here that when using the values plan, the resulting dataframe will be quite small
        if rows:
            df = pd.DataFrame(rows)
        else:
            field_names = [str(x) for x in rel.getRowType().getFieldNames()]
            df = pd.DataFrame(columns=field_names)

        df = dd.from_pandas(df, npartitions=1)
        cc = ColumnContainer(df.columns)

        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        dc = DataContainer(df, cc)
        dc = self.fix_dtype_to_row_type(dc, rel.getRowType())
        return dc
