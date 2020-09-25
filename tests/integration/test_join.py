import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from tests.integration.fixtures import DaskTestCase


class JoinTestCase(DaskTestCase):
    def test_join(self):
        df = self.c.sql(
            "SELECT lhs.user_id, lhs.b, rhs.c FROM user_table_1 AS lhs JOIN user_table_2 AS rhs ON lhs.user_id = rhs.user_id"
        )
        df = df.compute()

        expected_df = pd.DataFrame(
            {"user_id": [1, 1, 2, 2], "b": [3, 3, 1, 3], "c": [1, 2, 3, 3]}
        )
        assert_frame_equal(
            df.sort_values(["user_id", "b", "c"]).reset_index(drop=True), expected_df,
        )

    def test_join_inner(self):
        df = self.c.sql(
            "SELECT lhs.user_id, lhs.b, rhs.c FROM user_table_1 AS lhs INNER JOIN user_table_2 AS rhs ON lhs.user_id = rhs.user_id"
        )
        df = df.compute()

        expected_df = pd.DataFrame(
            {"user_id": [1, 1, 2, 2], "b": [3, 3, 1, 3], "c": [1, 2, 3, 3]}
        )
        assert_frame_equal(
            df.sort_values(["user_id", "b", "c"]).reset_index(drop=True), expected_df,
        )

    def test_join_outer(self):
        df = self.c.sql(
            "SELECT lhs.user_id, lhs.b, rhs.c FROM user_table_1 AS lhs FULL JOIN user_table_2 AS rhs ON lhs.user_id = rhs.user_id"
        )
        df = df.compute()

        expected_df = pd.DataFrame(
            {
                # That is strange. Unfortunately, it seems dask fills in the
                # missing rows with NaN, not with NA...
                "user_id": [1, 1, 2, 2, 3, np.NaN],
                "b": [3, 3, 1, 3, 3, np.NaN],
                "c": [1, 2, 3, 3, np.NaN, 4],
            }
        )
        assert_frame_equal(
            df.sort_values(["user_id", "b", "c"]).reset_index(drop=True), expected_df
        )

    def test_join_left(self):
        df = self.c.sql(
            "SELECT lhs.user_id, lhs.b, rhs.c FROM user_table_1 AS lhs LEFT JOIN user_table_2 AS rhs ON lhs.user_id = rhs.user_id"
        )
        df = df.compute()

        expected_df = pd.DataFrame(
            {
                # That is strange. Unfortunately, it seems dask fills in the
                # missing rows with NaN, not with NA...
                "user_id": [1, 1, 2, 2, 3],
                "b": [3, 3, 1, 3, 3],
                "c": [1, 2, 3, 3, np.NaN],
            }
        )
        assert_frame_equal(
            df.sort_values(["user_id", "b", "c"]).reset_index(drop=True), expected_df,
        )

    def test_join_right(self):
        df = self.c.sql(
            "SELECT lhs.user_id, lhs.b, rhs.c FROM user_table_1 AS lhs RIGHT JOIN user_table_2 AS rhs ON lhs.user_id = rhs.user_id"
        )
        df = df.compute()

        expected_df = pd.DataFrame(
            {
                # That is strange. Unfortunately, it seems dask fills in the
                # missing rows with NaN, not with NA...
                "user_id": [1, 1, 2, 2, np.NaN],
                "b": [3, 3, 1, 3, np.NaN],
                "c": [1, 2, 3, 3, 4],
            }
        )
        assert_frame_equal(
            df.sort_values(["user_id", "b", "c"]).reset_index(drop=True), expected_df,
        )

    def test_join_strange(self):
        self.assertRaises(
            NotImplementedError,
            self.c.sql,
            "SELECT lhs.a, rhs.b FROM df_simple AS lhs JOIN df_simple AS rhs ON lhs.a = 3",
        )

    def test_join_complex(self):
        df = self.c.sql(
            "SELECT lhs.a, rhs.b FROM df_simple AS lhs JOIN df_simple AS rhs ON lhs.a < rhs.b",
        )
        df = df.compute()

        df_expected = pd.DataFrame(
            {"a": [1, 1, 1, 2, 2, 3], "b": [1.1, 2.2, 3.3, 2.2, 3.3, 3.3]}
        )

        assert_frame_equal(
            df.sort_values(["a", "b"]).reset_index(drop=True), df_expected
        )

        df = self.c.sql(
            """
                SELECT lhs.a, lhs.b, rhs.a, rhs.b
                FROM
                    df_simple AS lhs
                JOIN df_simple AS rhs
                ON lhs.a < rhs.b AND lhs.b < rhs.a
            """,
            debug=True,
        )
        df = df.compute()

        df_expected = pd.DataFrame(
            {
                "a": [1, 1, 2],
                "b": [1.1, 1.1, 2.2],
                "a0": [2, 3, 3],
                "b0": [2.2, 3.3, 3.3],
            }
        )

        assert_frame_equal(
            df.sort_values(["a", "b0"]).reset_index(drop=True), df_expected
        )

    def test_join_complex_2(self):
        df = self.c.sql(
            """
        SELECT
            lhs.user_id, lhs.b, rhs.user_id, rhs.c
        FROM user_table_1 AS lhs
        JOIN user_table_2 AS rhs
            ON rhs.user_id = lhs.user_id AND rhs.c - lhs.b >= 0
        """
        )

        df = df.compute()

        df_expected = pd.DataFrame(
            {"user_id": [2, 2], "b": [1, 3], "user_id0": [2, 2], "c": [3, 3]}
        )

        assert_frame_equal(df.sort_values("b").reset_index(drop=True), df_expected)
