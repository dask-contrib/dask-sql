import pandas as pd
from pandas.testing import assert_frame_equal

from tests.integration.fixtures import DaskTestCase


class GroupbyTestCase(DaskTestCase):
    def test_group_by(self):
        df = self.c.sql(
            """
        SELECT
            user_id, SUM(c) AS S
        FROM user_table_1
        GROUP BY user_id
        """
        )
        df = df.compute()

        expected_df = pd.DataFrame({"user_id": [2, 1], "S": [4, 3]})
        assert_frame_equal(df, expected_df)

    def test_group_by_all(self):
        df = self.c.sql(
            """
        SELECT
            SUM(c) AS S, SUM(2) AS X
        FROM user_table_1
        """
        )
        df = df.compute()

        expected_df = pd.DataFrame({"S": [7], "X": [6]})
        assert_frame_equal(df, expected_df, check_dtype=False)

    def test_group_by_case(self):
        df = self.c.sql(
            """
        SELECT
            user_id, SUM(CASE WHEN c = 3 THEN 1 END) AS S
        FROM user_table_1
        GROUP BY user_id
        """
        )
        df = df.compute()

        expected_df = pd.DataFrame({"user_id": [2, 1], "S": [1, 1]})
        assert_frame_equal(df, expected_df)
