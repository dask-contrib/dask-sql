from dask_sql.physical.ral import (
    convert_ral_to_df,
    fix_column_to_row_type,
    check_columns_from_row_type,
)
from dask_sql.physical.rex import apply_rex_call


class LogicalSortPlugin:
    class_name = "org.apache.calcite.rel.logical.LogicalSort"

    def __call__(self, ral, tables):
        assert len(ral.getInputs()) == 1

        input_ral = ral.getInput()
        df = convert_ral_to_df(input_ral, tables)
        check_columns_from_row_type(df, ral.getExpectedInputRowType(0))

        sort_collation = ral.getCollation().getFieldCollations()
        sort_columns = [df.columns[int(x.getFieldIndex())] for x in sort_collation]
        sort_ascending = [str(x.getDirection()) == "ASCENDING" for x in sort_collation]

        # Split the first column. We need to handle this one with set_index
        first_sort_column = sort_columns[0]
        first_sort_ascending = sort_ascending[0]

        # Sort the first column with set_index. Currently, we can only handle ascending sort
        if not first_sort_ascending:
            raise NotImplementedError(
                "The first column needs to be sorted ascending (yet)"
            )
        df_sorted = df.set_index(first_sort_column, drop=False).reset_index(drop=True)

        # sort the remaining columns if given
        if sort_columns:
            sort_parition_func = lambda x: x.reset_index(drop=True).sort_values(
                sort_columns, ascending=sort_ascending
            )
            df_sorted = df_sorted.map_partitions(
                sort_parition_func, meta=df_sorted._meta
            )

        df_sorted = fix_column_to_row_type(df_sorted, ral.getRowType())

        return df_sorted
