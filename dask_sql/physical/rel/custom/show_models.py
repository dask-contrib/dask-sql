from typing import TYPE_CHECKING

import dask.dataframe as dd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.utils import get_serial_library

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan


class ShowModelsPlugin(BaseRelPlugin):
    """
    Show all MODELS currently registered/trained.
    The SQL is:

        SHOW MODELS

    The result is also a table, although it is created on the fly.
    """

    class_name = "ShowModels"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        schema_name = rel.show_models().getSchemaName() or context.schema_name

        xd = get_serial_library(context.gpu)
        df = xd.DataFrame({"Models": list(context.schema[schema_name].models.keys())})

        cc = ColumnContainer(df.columns)
        dc = DataContainer(dd.from_pandas(df, npartitions=1), cc)
        return dc
