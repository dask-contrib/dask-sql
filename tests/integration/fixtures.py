from unittest import TestCase

import dask.dataframe as dd
import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np

from dask_sql.context import Context


class DaskTestCase(TestCase):
    def setUp(self):
        self.df_simple = pd.DataFrame({"a": [1, 2, 3], "b": [1.1, 2.2, 3.3]})
        self.df = pd.DataFrame(
            {"a": [1] * 100 + [2] * 200 + [3] * 400, "b": 10 * np.random.rand(700)}
        )
        self.user_table_1 = pd.DataFrame({"user_id": [2, 1, 2, 3], "b": [3, 3, 1, 3]})
        self.user_table_2 = pd.DataFrame({"user_id": [1, 1, 2, 4], "c": [1, 2, 3, 4]})
        self.long_table = pd.DataFrame({"a": [0] * 100 + [1] * 101 + [2] * 103})

        self.user_table_inf = pd.DataFrame({"c": [3, float("inf"), 1]})
        self.user_table_nan = pd.DataFrame({"c": [3, float("nan"), 1]})

        self.c = Context()
        for df_name in [
            "df",
            "df_simple",
            "user_table_1",
            "user_table_2",
            "long_table",
            "user_table_inf",
            "user_table_nan",
        ]:
            df = getattr(self, df_name)
            dask_df = dd.from_pandas(df, npartitions=3)
            self.c.register_dask_table(dask_df, df_name)
