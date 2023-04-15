import logging
from typing import TYPE_CHECKING

from dask_planner.rust import RexType
from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.rex import RexConverter
from dask_sql.utils import new_temporary_column

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan

logger = logging.getLogger(__name__)


class DaskProjectPlugin(BaseRelPlugin):
    """
    A DaskProject is used to
    (a) apply expressions to the columns and
    (b) only select a subset of the columns
    """

    class_name = "Projection"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        # Get the input of the previous step
        (dc,) = self.assert_inputs(rel, 1, context)

        df = dc.df
        cc = dc.column_container

        # Collect all (new) columns
        proj = rel.projection()
        named_projects = proj.getNamedProjects()

        column_names = []
        new_columns = {}
        new_mappings = {}

        # Collect all (new) columns this Projection will limit to
        for key, expr in named_projects:
            key = str(key)
            column_names.append(key)

            # shortcut: if we have a column already, there is no need to re-assign it again
            # this is only the case if the expr is a RexInputRef
            if expr.getRexType() == RexType.Reference:
                index = expr.getIndex()
                backend_column_name = cc.get_backend_by_frontend_index(index)
                logger.debug(
                    f"Not re-adding the same column {key} (but just referencing it)"
                )
                new_mappings[key] = backend_column_name
            else:
                random_name = new_temporary_column(df)
                new_columns[random_name] = RexConverter.convert(
                    rel, expr, dc, context=context
                )
                logger.debug(f"Adding a new column {key} out of {expr}")
                new_mappings[key] = random_name

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
