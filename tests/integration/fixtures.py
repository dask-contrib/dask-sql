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
            {
                "a": [1.0] * 100 + [2.0] * 200 + [3.0] * 400,
                "b": 10 * np.random.rand(700),
            }
        )
        self.user_table_1 = pd.DataFrame({"user_id": [2, 1, 2, 3], "b": [3, 3, 1, 3]})
        self.user_table_2 = pd.DataFrame({"user_id": [1, 1, 2, 4], "c": [1, 2, 3, 4]})
        self.long_table = pd.DataFrame({"a": [0] * 100 + [1] * 101 + [2] * 103})

        self.user_table_inf = pd.DataFrame({"c": [3, float("inf"), 1]})
        self.user_table_nan = pd.DataFrame({"c": [3, pd.NA, 1]})
        self.string_table = pd.DataFrame({"a": ["a normal string", "%_%", "^|()-*[]$"]})

        self.c = Context()
        for df_name in [
            "df",
            "df_simple",
            "user_table_1",
            "user_table_2",
            "long_table",
            "user_table_inf",
            "user_table_nan",
            "string_table",
        ]:
            df = getattr(self, df_name)
            dask_df = dd.from_pandas(df, npartitions=3)
            self.c.register_dask_table(dask_df, df_name)


class ComparisonTestCase(TestCase):
    def setUp(self):
        np.random.seed(42)

        df1 = dd.from_pandas(
            pd.DataFrame(
                {
                    "user_id": np.random.choice([1, 2, 3, 4, pd.NA], 100),
                    "a": np.random.rand(100),
                    "b": np.random.randint(-10, 10, 100),
                }
            ),
            npartitions=3,
        )
        df1["user_id"] = df1["user_id"].astype("Int64")

        df2 = dd.from_pandas(
            pd.DataFrame(
                {
                    "user_id": np.random.choice([1, 2, 3, 4], 100),
                    "c": np.random.randint(20, 30, 100),
                    "d": np.random.choice(["a", "b", "c"], 100),
                }
            ),
            npartitions=3,
        )

        df3 = dd.from_pandas(
            pd.DataFrame(
                {
                    "s": [
                        "".join(np.random.choice(["a", "B", "c", "D"], 10))
                        for _ in range(100)
                    ]
                }
            ),
            npartitions=3,
        )

        # the other is a Int64, that makes joining simpler
        df2["user_id"] = df2["user_id"].astype("Int64")

        # add some NaNs
        df1["a"] = df1["a"].apply(
            lambda a: float("nan") if a > 0.8 else a, meta=("a", "float")
        )
        df1["b_bool"] = df1["b"].apply(
            lambda b: pd.NA if b > 5 else b < 0, meta=("a", "bool")
        )

        self.c = Context()
        self.c.register_dask_table(df1, "df1")
        self.c.register_dask_table(df2, "df2")
        self.c.register_dask_table(df3, "df3")

        df1.compute().to_sql("df1", self.engine, index=False, if_exists="replace")
        df2.compute().to_sql("df2", self.engine, index=False, if_exists="replace")
        df3.compute().to_sql("df3", self.engine, index=False, if_exists="replace")

    def assert_query_gives_same_result(self, query, sort_columns=None, **kwargs):
        sql_result = pd.read_sql_query(query, self.engine)
        dask_result = self.c.sql(query).compute()

        # allow that the names are different
        # as expressions are handled differently
        dask_result.columns = sql_result.columns

        if sort_columns:
            sql_result = sql_result.sort_values(sort_columns)
            dask_result = dask_result.sort_values(sort_columns)

        sql_result = sql_result.reset_index(drop=True)
        dask_result = dask_result.reset_index(drop=True)

        assert_frame_equal(sql_result, dask_result, check_dtype=False, **kwargs)
