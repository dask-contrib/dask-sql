from typing import TYPE_CHECKING

import dask.dataframe as dd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_sql.java import org


class DaskUnionPlugin(BaseRelPlugin):
    """
    DaskUnion is used on UNION clauses.
    It just concatonates the two data frames.
    """

    class_name = "com.dask.sql.nodes.DaskUnion"

    def convert(
        self, rel: "org.apache.calcite.rel.RelNode", context: "dask_sql.Context"
    ) -> DataContainer:
        first_dc, second_dc = self.assert_inputs(rel, 2, context)

        first_df = first_dc.df
        first_cc = first_dc.column_container

        second_df = second_dc.df
        second_cc = second_dc.column_container

        # For concatenating, they should have exactly the same fields
        output_field_names = [str(x) for x in rel.getRowType().getFieldNames()]
        assert len(first_cc.columns) == len(output_field_names)
        first_cc = first_cc.rename(
            columns={
                col: output_col
                for col, output_col in zip(first_cc.columns, output_field_names)
            }
        )
        first_dc = DataContainer(first_df, first_cc)

        assert len(second_cc.columns) == len(output_field_names)
        second_cc = second_cc.rename(
            columns={
                col: output_col
                for col, output_col in zip(second_cc.columns, output_field_names)
            }
        )
        second_dc = DataContainer(second_df, second_cc)

        # To concat the to dataframes, we need to make sure the
        # columns actually have the specified names in the
        # column containers
        # Otherwise the concat won't work
        first_df = first_dc.assign()
        second_df = second_dc.assign()

        self.check_columns_from_row_type(first_df, rel.getExpectedInputRowType(0))
        self.check_columns_from_row_type(second_df, rel.getExpectedInputRowType(1))

        df = dd.concat([first_df, second_df])

        if not rel.all:
            df = df.drop_duplicates()

        cc = ColumnContainer(df.columns)
        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        dc = DataContainer(df, cc)
        dc = self.fix_dtype_to_row_type(dc, rel.getRowType())
        return dc
