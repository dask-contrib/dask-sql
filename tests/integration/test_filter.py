from pandas.testing import assert_frame_equal

from tests.integration.fixtures import DaskTestCase


class FilterTestCase(DaskTestCase):
    def test_filter(self):
        df = self.c.sql("SELECT * FROM df WHERE a < 2")
        df = df.compute()

        expected_df = self.df[self.df["a"] < 2]
        assert_frame_equal(df, expected_df)

    def test_filter_complicated(self):
        df = self.c.sql("SELECT * FROM df WHERE a < 3 AND (b > 1 AND b < 3)")
        df = df.compute()

        expected_df = self.df[
            ((self.df["a"] < 3) & ((self.df["b"] > 1) & (self.df["b"] < 3)))
        ]
        assert_frame_equal(
            df, expected_df,
        )
