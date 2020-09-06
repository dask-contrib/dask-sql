from typing import Dict

import dask.dataframe as dd

from dask_sql.physical.rex import RexConverter
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import DataContainer


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
        (dc,) = self.assert_inputs(rel, 1, tables)

        df = dc.df
        cc = dc.column_container

        # Collect all new columns
        named_projects = rel.getNamedProjects()

        # TODO: we do not need to create a new column if we already have it
        new_columns = {}
        for expr, key in named_projects:
            new_columns[str(key)] = RexConverter.convert(expr, dc=dc)

        df = df.assign(**new_columns)
        for new_column in new_columns:
            cc = cc.add(new_column)

        # Make sure the order is correct
        column_names = list(new_columns.keys())
        cc = cc.limit_to(column_names)

        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        return DataContainer(df, cc)
