import logging
from uuid import uuid4

from dask_sql.datacontainer import DataContainer
from dask_sql.java import org
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.rex import RexConverter
from dask_sql.physical.rex.base import ScalarValue
from dask_sql.utils import new_temporary_column

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

        # Collect all (new) columns
        named_projects = rel.getNamedProjects()

        column_names = []
        mappings = {}

        # side-note: there is a trade-off involved here.
        # If the result is not a scalar, we add the
        # column directly within the plugin, therefore
        # changing the dataframe (and container). We do this
        # to allow plugins to change more than that, e.g.
        # to reorder the rows if needed (and more optimal).
        # That makes OVER much more performant, but also
        # comes with the tradeoff that columns are now added
        # sequentially, not all at once. If this is a problem
        # needs to be studied (as optimization might kick in).
        for expr, key in named_projects:
            key = str(key)
            column_names.append(key)

            # Let the rex converter to the real magic
            # we only do some book-keeping here
            logger.debug(f"Adding a new column {key} out of {expr}")
            new_column, dc = RexConverter.convert_to_column_reference(
                expr, dc, context=context
            )

            # Important: do not dereference the column here,
            # as the dataframe might still change (in future iterations)
            mappings[key] = new_column._column_name

        # Make sure the order is correct and name the newly added
        # columns correctly
        df = dc.df
        cc = dc.column_container
        for frontend, backend in mappings.items():
            # Note: We can not just do a rename, as we might have doubled columns
            cc = cc.add(frontend, backend)
        cc = cc.limit_to(column_names)

        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        dc = DataContainer(df, cc)
        dc = self.fix_dtype_to_row_type(dc, rel.getRowType())
        return dc
