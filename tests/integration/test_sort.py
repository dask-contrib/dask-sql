from pandas.testing import assert_frame_equal

from tests.integration.fixtures import DaskTestCase


class SortTestCase(DaskTestCase):
    def test_sort(self):
        df = self.c.sql(
            """
        SELECT
            *
        FROM user_table_1
        ORDER BY c, user_id DESC
        """
        )
        df = df.compute().reset_index(drop=True)
        df_expected = self.user_table_1.sort_values(
            ["c", "user_id"], ascending=[True, False]
        ).reset_index(drop=True)

        assert_frame_equal(df, df_expected)

    def test_sort_not_allowed(self):
        self.assertRaises(
            NotImplementedError,
            self.c.sql,
            "SELECT * FROM user_table_1 ORDER BY c DESC",
        )


class LimitTestCase(DaskTestCase):
    def test_limit(self):
        df = self.c.sql("SELECT * FROM long_table LIMIT 101")
        df = df.compute()

        assert_frame_equal(df, self.long_table.iloc[:101])

        df = self.c.sql("SELECT * FROM long_table LIMIT 100")
        df = df.compute()

        assert_frame_equal(df, self.long_table.iloc[:100])

        df = self.c.sql("SELECT * FROM long_table LIMIT 100 OFFSET 99")
        df = df.compute()

        assert_frame_equal(df, self.long_table.iloc[99 : 99 + 100])

        df = self.c.sql("SELECT * FROM long_table LIMIT 100 OFFSET 100")
        df = df.compute()

        assert_frame_equal(df, self.long_table.iloc[100 : 100 + 100])

        df = self.c.sql("SELECT * FROM long_table LIMIT 101 OFFSET 101")
        df = df.compute()

        assert_frame_equal(df, self.long_table.iloc[101 : 101 + 101])
