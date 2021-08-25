import dask.dataframe as dd
import pandas as pd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin


class DistributeByPlugin(BaseRelPlugin):
    """
    Distribute the target based on the specified sql identifier from a SELECT query.
    The SQL is:

        SELECT age, name FROM person DISTRIBUTE BY age

    HERE, What do we actually return here???
    """

    class_name = "com.dask.sql.parser.SqlDistributeBy"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:

        # cc = ColumnContainer(df.columns)
        # dc = DataContainer(df, cc)
        #
        # return dc

        # Just placeholder so I could pass pre-commit and share with others.
        print("DistributeByPlugin")
        return
