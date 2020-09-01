from unittest import TestCase

import dask.dataframe as dd
import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np

from dask_sql.context import Context


class DaskTestCase(TestCase):
    def setUp(self):
        self.df = pd.DataFrame({"a": [1, 2, 3], "b": [1.1, 2.2, 3.3]})
        self.user_table_1 = pd.DataFrame({"user_id": [2, 1, 2], "c": [3, 3, 1]})
        self.user_table_2 = pd.DataFrame({"user_id": [1, 1, 2], "b": [1, 2, 3]})
        self.long_table = pd.DataFrame({"a": [0] * 100 + [1] * 101 + [2] * 103})

        self.user_table_inf = pd.DataFrame({"c": [3, float("inf"), 1]})
        self.user_table_nan = pd.DataFrame({"c": [3, float("inf"), 1]})

        self.c = Context()
        self.c.register_dask_table(dd.from_pandas(self.df, npartitions=3), "df")
        self.c.register_dask_table(
            dd.from_pandas(self.user_table_1, npartitions=3), "user_table_1"
        )
        self.c.register_dask_table(
            dd.from_pandas(self.user_table_2, npartitions=3), "user_table_2"
        )
        self.c.register_dask_table(
            dd.from_pandas(self.long_table, npartitions=3), "long_table"
        )
        self.c.register_dask_table(
            dd.from_pandas(self.user_table_nan, npartitions=3), "user_table_nan"
        )
        self.c.register_dask_table(
            dd.from_pandas(self.user_table_inf, npartitions=3), "user_table_inf"
        )
