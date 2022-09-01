from typing import TYPE_CHECKING

from dask_sql.datacontainer import DataContainer
from dask_sql.java import org
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.utils.sort import apply_sort

if TYPE_CHECKING:
    import dask_sql


class DaskSortPlugin(BaseRelPlugin):
    """
    DaskSort is used to sort by columns (ORDER BY).
    """

    class_name = "com.dask.sql.nodes.DaskSort"

    def convert(
        self, rel: "org.apache.calcite.rel.RelNode", context: "dask_sql.Context"
    ) -> DataContainer:
        (dc,) = self.assert_inputs(rel, 1, context)
        df = dc.df
        cc = dc.column_container

        sort_collation = rel.getCollation().getFieldCollations()
        sort_columns = [
            cc.get_backend_by_frontend_index(int(x.getFieldIndex()))
            for x in sort_collation
        ]

        ASCENDING = org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING
        FIRST = org.apache.calcite.rel.RelFieldCollation.NullDirection.FIRST
        sort_ascending = [x.getDirection() == ASCENDING for x in sort_collation]
        sort_null_first = [x.nullDirection == FIRST for x in sort_collation]

        df = df.persist()
        df = apply_sort(df, sort_columns, sort_ascending, sort_null_first)

        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        # No column type has changed, so no need to cast again
        return DataContainer(df, cc)
