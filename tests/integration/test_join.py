import pandas as pd
from pandas.testing import assert_frame_equal

from tests.integration.fixtures import DaskTestCase


class JoinTestCase(DaskTestCase):
    def test_join(self):
        df = self.c.sql(
            "SELECT lhs.a, rhs.b from df AS lhs JOIN df AS rhs ON lhs.a = rhs.a"
        )
        df = df.compute()

        assert_frame_equal(df.reset_index(drop=True), self.df)

    def test_join_inner(self):
        df = self.c.sql(
            "SELECT lhs.a, rhs.b from df AS lhs INNER JOIN df AS rhs ON lhs.a = rhs.a"
        )
        df = df.compute()

        assert_frame_equal(df.reset_index(drop=True), self.df)

    def test_join_outer(self):
        df = self.c.sql(
            "SELECT lhs.a, rhs.b from df AS lhs FULL JOIN df AS rhs ON lhs.a = rhs.a"
        )
        df = df.compute()

        assert_frame_equal(df.reset_index(drop=True), self.df)

    def test_join_left(self):
        df = self.c.sql(
            "SELECT lhs.a, rhs.b from df AS lhs LEFT JOIN df AS rhs ON lhs.a = rhs.a"
        )
        df = df.compute()

        assert_frame_equal(df.reset_index(drop=True), self.df)

    def test_join_right(self):
        df = self.c.sql(
            "SELECT lhs.a, rhs.b from df AS lhs RIGHT JOIN df AS rhs ON lhs.a = rhs.a"
        )
        df = df.compute()

        assert_frame_equal(df.reset_index(drop=True), self.df)

    def test_join_strange(self):
        self.assertRaises(
            NotImplementedError,
            self.c.sql,
            "SELECT lhs.a, rhs.b from df AS lhs JOIN df AS rhs ON lhs.a = 3",
        )

    def test_join_complex(self):
        df = self.c.sql(
            "SELECT lhs.a, rhs.b from df AS lhs JOIN df AS rhs ON lhs.a < rhs.b",
        )
        df = df.compute()

        df_expected = pd.DataFrame(
            {"a": [1, 1, 1, 2, 2, 3], "b": [1.1, 2.2, 3.3, 2.2, 3.3, 3.3]}
        )

        assert_frame_equal(
            df.sort_values(["a", "b"]).reset_index(drop=True), df_expected
        )

    def test_join_complex_2(self):
        df = self.c.sql(
            """
        SELECT
            lhs.user_id, lhs.b, rhs.user_id, rhs.c
        FROM user_table_2 AS lhs
        JOIN user_table_1 AS rhs
            ON lhs.user_id = rhs.user_id AND lhs.b - rhs.c >= 0
        """
        )

        df = df.compute()

        df_expected = pd.DataFrame(
            {"user_id": [2, 2], "b": [3, 3], "user_id0": [2, 2], "c": [1, 3],}
        )

        assert_frame_equal(df.sort_values("c").reset_index(drop=True), df_expected)
