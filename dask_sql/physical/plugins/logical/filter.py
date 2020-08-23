from dask_sql.physical.ral import (
    convert_ral_to_df,
    fix_column_to_row_type,
    check_columns_from_row_type,
)
from dask_sql.physical.rex import apply_rex_call


class LogicalFilterPlugin:
    class_name = "org.apache.calcite.rel.logical.LogicalFilter"

    def __call__(self, ral, tables):
        assert len(ral.getInputs()) == 1

        input_ral = ral.getInput()
        df = convert_ral_to_df(input_ral, tables)
        check_columns_from_row_type(df, ral.getExpectedInputRowType(0))

        condition = ral.getCondition()
        df_condition = apply_rex_call(condition, df)

        df = df[df_condition]

        df = fix_column_to_row_type(df, ral.getRowType())

        return df
