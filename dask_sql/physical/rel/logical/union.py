from typing import Dict

import dask.dataframe as dd

from dask_sql.physical.rex import RexConverter
from dask_sql.physical.rel.base import BaseRelPlugin


class LogicalUnionPlugin(BaseRelPlugin):
    """
    LogicalUnion is used on UNION clauses.
    It just concatonates the two data frames.
    """

    class_name = "org.apache.calcite.rel.logical.LogicalUnion"

    def convert(
        self, rel: "org.apache.calcite.rel.RelNode", tables: Dict[str, dd.DataFrame]
    ) -> dd.DataFrame:
        first_df, second_df = self.assert_inputs(rel, 2, tables)

        # For concatenating, they should have exactly the same fields
        output_field_names = [str(x) for x in rel.getRowType().getFieldNames()]
        assert len(first_df.columns) == len(output_field_names)
        first_df = first_df.rename(
            columns={
                col: output_col
                for col, output_col in zip(first_df.columns, output_field_names)
            }
        )
        assert len(second_df.columns) == len(output_field_names)
        second_df = second_df.rename(
            columns={
                col: output_col
                for col, output_col in zip(second_df.columns, output_field_names)
            }
        )

        self.check_columns_from_row_type(first_df, rel.getExpectedInputRowType(0))
        self.check_columns_from_row_type(second_df, rel.getExpectedInputRowType(1))

        df = dd.concat([first_df, second_df])

        if not rel.all:
            df = df.drop_duplicates()

        df = self.fix_column_to_row_type(df, rel.getRowType())
        return df
