import dask.dataframe as dd
import pandas as pd

from dask_sql.physical.ral import convert_ral_to_df, fix_column_to_row_type
from dask_sql.physical.rex import apply_rex_call


class LogicalValuesPlugin:
    class_name = "org.apache.calcite.rel.logical.LogicalValues"

    def __call__(self, ral, tables):
        assert len(ral.getInputs()) == 0

        rex_expression_rows = list(ral.getTuples())
        rows = []
        for rex_expressions in rex_expression_rows:
            rows.append([apply_rex_call(rex, None) for rex in rex_expressions])

        # We assume here that when using the values plan, the resulting dataframe will be quite small
        # TODO: we explicitely reference pandas and dask here -> might we worth making this more general
        df = dd.from_pandas(pd.DataFrame(rows), npartitions=1)
        df = fix_column_to_row_type(df, ral.getRowType())

        return df
