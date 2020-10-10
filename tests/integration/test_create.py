import os
import tempfile
from pandas.testing import assert_frame_equal

from tests.integration.fixtures import DaskTestCase


class CreateTestCase(DaskTestCase):
    def setUp(self):
        super().setUp()

        self.f = os.path.join(tempfile.gettempdir(), os.urandom(24).hex())

    def tearDown(self):
        super().tearDown()

        if os.path.exists(self.f):
            os.unlink(self.f)

    def test_create_from_csv(self):
        self.df.to_csv(self.f, index=False)

        self.c.sql(
            f"""
            CREATE TABLE
                new_table
            WITH (
                location = '{self.f}',
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
        self.df.to_csv(self.f, index=False)

        self.c.sql(
            f"""
            CREATE TABLE
                new_table
            WITH (
                location = '{self.f}',
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
