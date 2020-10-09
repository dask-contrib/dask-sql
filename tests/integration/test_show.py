import pandas as pd
from pandas.testing import assert_frame_equal

from tests.integration.fixtures import DaskTestCase


class ShowTestCase(DaskTestCase):
    def test_schemas(self):
        df = self.c.sql("SHOW SCHEMAS")
        df = df.compute()

        expected_df = pd.DataFrame({"Schema": [self.c.schema_name]})

        assert_frame_equal(df, expected_df)

    def test_tables(self):
        df = self.c.sql(f'SHOW TABLES FROM "{self.c.schema_name}"')
        df = df.compute()

        expected_df = pd.DataFrame(
            {
                "Table": [
                    "df",
                    "df_simple",
                    "user_table_1",
                    "user_table_2",
                    "long_table",
                    "user_table_inf",
                    "user_table_nan",
                    "string_table",
                ]
            }
        )

        assert_frame_equal(df.sort_values("Table"), expected_df.sort_values("Table"))

    def test_columns(self):
        df = self.c.sql(f'SHOW COLUMNS FROM "{self.c.schema_name}"."user_table_1"')
        df = df.compute()

        expected_df = pd.DataFrame(
            {
                "Column": ["user_id", "b",],
                "Type": ["bigint", "bigint"],
                "Extra": [""] * 2,
                "Comment": [""] * 2,
            }
        )

        assert_frame_equal(df.sort_values("Column"), expected_df.sort_values("Column"))

        df = self.c.sql('SHOW COLUMNS FROM "user_table_1"')
        df = df.compute()
        assert_frame_equal(df.sort_values("Column"), expected_df.sort_values("Column"))

    def test_wrong_input(self):
        self.assertRaises(
            AttributeError, self.c.sql, 'SHOW COLUMNS FROM "wrong"."table"'
        )
        self.assertRaises(
            AttributeError, self.c.sql, 'SHOW COLUMNS FROM "wrong"."table"."column"'
        )
        self.assertRaises(
            KeyError, self.c.sql, f'SHOW COLUMNS FROM "{self.c.schema_name}"."table"'
        )
        self.assertRaises(AttributeError, self.c.sql, 'SHOW TABLES FROM "wrong"')
