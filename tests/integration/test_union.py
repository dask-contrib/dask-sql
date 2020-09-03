import pandas as pd
from pandas.testing import assert_frame_equal

from tests.integration.fixtures import DaskTestCase


class UnionTestCase(DaskTestCase):
    def test_union_not_all(self):
        df = self.c.sql(
            """
            SELECT * FROM df
            UNION
            SELECT * FROM df
            UNION
            SELECT * FROM df
            """
        )
        df = df.compute()

        assert_frame_equal(df.reset_index(drop=True), self.df)

    def test_union_all(self):
        df = self.c.sql(
            """
            SELECT * FROM df
            UNION ALL
            SELECT * FROM df
            UNION ALL
            SELECT * FROM df
            """
        )
        df = df.compute()

        expected_df = pd.concat([self.df, self.df, self.df], ignore_index=True)
        assert_frame_equal(df.reset_index(drop=True), expected_df)

    def test_union_mixed(self):
        df = self.c.sql(
            """
            SELECT a AS I, b as II FROM df
            UNION ALL
            SELECT a as I, a as II FROM long_table
            """
        )
        df = df.compute()

        long_table = self.long_table.rename(columns={"a": "I"})
        long_table["II"] = long_table["I"]

        expected_df = pd.concat(
            [self.df.rename(columns={"a": "I", "b": "II"}), long_table],
            ignore_index=True,
        )
        assert_frame_equal(df.reset_index(drop=True), expected_df)
