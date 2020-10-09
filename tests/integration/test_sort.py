from pandas.testing import assert_frame_equal

from tests.integration.fixtures import DaskTestCase
import pandas as pd
import dask.dataframe as dd


class SortTestCase(DaskTestCase):
    def test_sort(self):
        df = self.c.sql(
            """
        SELECT
            *
        FROM user_table_1
        ORDER BY b, user_id DESC
        """
        )
        df = df.compute().reset_index(drop=True)
        df_expected = self.user_table_1.sort_values(
            ["b", "user_id"], ascending=[True, False]
        ).reset_index(drop=True)

        assert_frame_equal(df, df_expected)

    def test_sort_with_nan(self):
        self.assertRaises(
            ValueError,
            self.c.sql,
            """
            SELECT
                *
            FROM user_table_nan
            ORDER BY c
        """,
        )

        self.assertRaises(
            ValueError,
            self.c.sql,
            """
            SELECT
                *
            FROM user_table_inf
            ORDER BY c
        """,
        )

    def test_sort_strings(self):
        string_table = pd.DataFrame({"a": ["zzhsd", "öfjdf", "baba"]})
        self.c.register_dask_table(
            dd.from_pandas(string_table, npartitions=1), "string_table"
        )

        df = self.c.sql(
            """
        SELECT
            *
        FROM string_table
        ORDER BY a
        """
        )
        df = df.compute().reset_index(drop=True)
        df_expected = string_table.sort_values(["a"], ascending=True).reset_index(
            drop=True
        )

        assert_frame_equal(df, df_expected)

    def test_sort_not_allowed(self):
        # No DESC implemented for the first column
        self.assertRaises(
            NotImplementedError,
            self.c.sql,
            "SELECT * FROM user_table_1 ORDER BY b DESC",
        )

        # Wrong column
        self.assertRaises(
            Exception, self.c.sql, "SELECT * FROM user_table_1 ORDER BY 42",
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
