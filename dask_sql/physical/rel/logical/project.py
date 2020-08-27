from typing import Dict

import dask.dataframe as dd

from dask_sql.physical.rex import RexConverter
from dask_sql.physical.rel.base import BaseRelPlugin


class LogicalProjectPlugin(BaseRelPlugin):
    """
    A LogicalProject is used to
    (a) apply expressions to the columns and
    (b) only select a subset of the columns
    """

    class_name = "org.apache.calcite.rel.logical.LogicalProject"

    def convert(
        self, rel: "org.apache.calcite.rel.RelNode", tables: Dict[str, dd.DataFrame]
    ) -> dd.DataFrame:
        # Get the input of the previous step
        (df,) = self.assert_inputs(rel, 1, tables)

        # It is easiest to just replace all columns with the new ones
        named_projects = rel.getNamedProjects()

        new_columns = {}
        for expr, key in named_projects:
            new_columns[str(key)] = RexConverter.convert(expr, df)

        df = df.drop(columns=list(df.columns)).assign(**new_columns)

        # Make sure the order is correct
        column_names = list(new_columns.keys())
        df = df[column_names]

        df = self.fix_column_to_row_type(df, rel.getRowType())
        return df
