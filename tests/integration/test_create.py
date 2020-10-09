import tempfile
from pandas.testing import assert_frame_equal

from tests.integration.fixtures import DaskTestCase


class CreateTestCase(DaskTestCase):
    def test_create_from_csv(self):
        with tempfile.NamedTemporaryFile() as f:
            self.df.to_csv(f.name, index=False)

            self.c.sql(
                f"""
                CREATE TABLE
                    new_table
                WITH (
                    location = '{f.name}',
                    format = 'csv'
                )
            """
            )

            df = self.c.sql(
                """
                SELECT * FROM new_table
            """
            ).compute()

            assert_frame_equal(self.df, df)

    def test_create_from_csv_persist(self):
        with tempfile.NamedTemporaryFile() as f:
            self.df.to_csv(f.name, index=False)

            self.c.sql(
                f"""
                CREATE TABLE
                    new_table
                WITH (
                    location = '{f.name}',
                    format = 'csv',
                    persist = True
                )
            """
            )

            df = self.c.sql(
                """
                SELECT * FROM new_table
            """
            ).compute()

            assert_frame_equal(self.df, df)

    def test_wrong_create(self):
        self.assertRaises(
            AttributeError,
            self.c.sql,
            f"""
                CREATE TABLE
                    new_table
                WITH (
                    format = 'csv'
                )
            """,
        )

        self.assertRaises(
            AttributeError,
            self.c.sql,
            f"""
                CREATE TABLE
                    new_table
                WITH (
                    format = 'strange',
                    location = 'some/path'
                )
            """,
        )
