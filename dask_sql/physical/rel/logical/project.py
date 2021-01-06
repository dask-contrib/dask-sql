import logging

from dask_sql.physical.rex import RexConverter
from dask_sql.physical.rex.core.input_ref import RexInputRefPlugin
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import DataContainer
from dask_sql.java import org

logger = logging.getLogger(__name__)


class LogicalProjectPlugin(BaseRelPlugin):
    """
    A LogicalProject is used to
    (a) apply expressions to the columns and
    (b) only select a subset of the columns
    """

    class_name = "org.apache.calcite.rel.logical.LogicalProject"

    def convert(
        self, rel: "org.apache.calcite.rel.RelNode", context: "dask_sql.Context"
    ) -> DataContainer:
        # Get the input of the previous step
        (dc,) = self.assert_inputs(rel, 1, context)

        df = dc.df
        cc = dc.column_container

        # Collect all (new) columns
        named_projects = rel.getNamedProjects()

        column_names = []
        new_columns = {}
        new_mappings = {}
        for expr, key in named_projects:
            key = str(key)
            column_names.append(key)

            # shortcut: if we have a column already, there is no need to re-assign it again
            # this is only the case if the expr is a RexInputRef
            if isinstance(expr, org.apache.calcite.rex.RexInputRef):
                index = expr.getIndex()
                backend_column_name = cc.get_backend_by_frontend_index(index)
                logger.debug(
                    f"Not re-adding the same column {key} (but just referencing it)"
                )
                new_mappings[key] = backend_column_name
            else:
                new_columns[key] = RexConverter.convert(expr, dc, context=context)
                logger.debug(f"Adding a new column {key} out of {expr}")
                cc = cc.add(key, key)
                new_mappings[key] = key

        # Actually add the new columns
        if new_columns:
            df = df.assign(**new_columns)

        # and the new mappings
        for key, backend_column_name in new_mappings.items():
            cc = cc.add(key, backend_column_name)

        # Make sure the order is correct
        cc = cc.limit_to(column_names)

        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        dc = DataContainer(df, cc)
        dc = self.fix_dtype_to_row_type(dc, rel.getRowType())
        return dc
