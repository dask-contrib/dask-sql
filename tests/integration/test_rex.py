import numpy as np
import pandas as pd
import dask.dataframe as dd
from pandas.testing import assert_frame_equal

from tests.integration.fixtures import DaskTestCase


class RexOperationsTestCase(DaskTestCase):
    def test_case(self):
        df = self.c.sql(
            """
        SELECT
            (CASE WHEN a = 3 THEN 1 END) AS S1,
            (CASE WHEN a > 0 THEN a ELSE 1 END) AS S2,
            (CASE WHEN a = 4 THEN 3 ELSE a + 1 END) AS S3,
            (CASE WHEN a = 3 THEN 1 ELSE a END) AS S4
        FROM df
        """
        )
        df = df.compute()

        expected_df = pd.DataFrame(index=self.df.index)
        expected_df["S1"] = self.df.a.apply(lambda a: 1 if a == 3 else None)
        expected_df["S2"] = self.df.a.apply(lambda a: a if a > 0 else 1)
        expected_df["S3"] = self.df.a.apply(lambda a: 3 if a == 4 else a + 1)
        expected_df["S4"] = self.df.a.apply(lambda a: 1 if a == 3 else a)
        assert_frame_equal(df, expected_df)

    def test_literals(self):
        df = self.c.sql("""SELECT 'a string äö' AS S, 4.4 AS F, -4564347464 AS I """)
        df = df.compute()

        expected_df = pd.DataFrame(
            {"S": ["a string äö"], "F": [4.4], "I": [-4564347464]}
        )
        assert_frame_equal(df, expected_df)

    def test_literal_null(self):
        df = self.c.sql("""SELECT NULL AS N, 1 + NULL AS I""")
        df = df.compute()

        expected_df = pd.DataFrame({"N": [None], "I": [np.nan]})
        assert_frame_equal(df, expected_df)

    def test_like(self):
        expected_df = pd.DataFrame({"a": ["a normal string", "%_%", "^|()-*[]$"]})
        self.c.register_dask_table(
            dd.from_pandas(expected_df, npartitions=1), "string_table"
        )

        df = self.c.sql(
            """
            SELECT * FROM string_table
            WHERE a LIKE '%n[a-z]rmal st_i%'
        """
        ).compute()

        assert_frame_equal(df, expected_df.iloc[[0]])

        df = self.c.sql(
            """
            SELECT * FROM string_table
            WHERE a LIKE 'Ä%Ä_Ä%' ESCAPE 'Ä'
        """
        ).compute()

        assert_frame_equal(df, expected_df.iloc[[1]])

        df = self.c.sql(
            """
            SELECT * FROM string_table
            WHERE a LIKE '^|()-*r[r]$' ESCAPE 'r'
        """
        ).compute()

        assert_frame_equal(df, expected_df.iloc[[2]])

        df = self.c.sql(
            """
            SELECT * FROM string_table
            WHERE a LIKE '%_' ESCAPE 'r'
        """
        ).compute()

        assert_frame_equal(df, expected_df)
