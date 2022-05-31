from typing import TYPE_CHECKING

import dask.dataframe as dd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan


def _extract_df(obj_cc, obj_df, output_field_names):
    # For concatenating, they should have exactly the same fields
    assert len(obj_cc.columns) == len(output_field_names)
    obj_cc = obj_cc.rename(
        columns={
            col: output_col
            for col, output_col in zip(obj_cc.columns, output_field_names)
        }
    )
    obj_dc = DataContainer(obj_df, obj_cc)
    return obj_dc.assign()


class DaskUnionPlugin(BaseRelPlugin):
    """
    DaskUnion is used on UNION clauses.
    It just concatonates the two data frames.
    """

    class_name = "Union"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        # Late import to remove cycling dependency
        from dask_sql.physical.rel.convert import RelConverter

        objs_dc = [
            RelConverter.convert(input_rel, context) for input_rel in rel.get_inputs()
        ]

        objs_df = [obj.df for obj in objs_dc]
        objs_cc = [obj.column_container for obj in objs_dc]

        output_field_names = [str(x) for x in rel.getRowType().getFieldNames()]
        obj_dfs = []
        for i, obj_df in enumerate(objs_df):
            obj_dfs.append(
                _extract_df(
                    obj_cc=objs_cc[i],
                    obj_df=obj_df,
                    output_field_names=output_field_names,
                )
            )

        _ = [self.check_columns_from_row_type(df, rel.getRowType()) for df in obj_dfs]

        df = dd.concat(obj_dfs)

        cc = ColumnContainer(df.columns)
        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        dc = DataContainer(df, cc)
        dc = self.fix_dtype_to_row_type(dc, rel.getRowType())
        return dc
