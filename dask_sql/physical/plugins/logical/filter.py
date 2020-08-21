from dask_sql.physical.ral import convert_ral_to_df
from dask_sql.physical.rex import apply_rex_call

class LogicalFilterPlugin:
    class_name = "org.apache.calcite.rel.logical.LogicalFilter"

    def __call__(self, ral, tables):
        input_ral = ral.getInput()
        df = convert_ral_to_df(input_ral, tables)

        variable_set = ral.getVariablesSet()
        assert not variable_set, "VariablesSet is not implemented so far"

        condition = ral.getCondition()
        df_condition = apply_rex_call(condition, df)

        return df[df_condition]
