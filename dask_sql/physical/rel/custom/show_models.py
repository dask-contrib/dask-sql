import dask.dataframe as dd
import pandas as pd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin


class ShowModelsPlugin(BaseRelPlugin):
    """
    Show all MODELS currently registered/trained.
    The SQL is:

        SHOW MODELS

    The result is also a table, although it is created on the fly.
    """

    class_name = "com.dask.sql.parser.SqlShowModels"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:

        df = pd.DataFrame(
            {"Models": list(context.schema[context.schema_name].models.keys())}
        )

        cc = ColumnContainer(df.columns)
        dc = DataContainer(dd.from_pandas(df, npartitions=1), cc)
        return dc
