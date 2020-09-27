from unittest import TestCase

from dask.datasets import timeseries

from dask_sql import Context


class TimeSeriesTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.c = Context()

        df = timeseries(freq="1d").persist()
        cls.c.register_dask_table(df, "timeseries")

    def test_complex_query(self):
        result = self.c.sql(
            """
            SELECT
                lhs.name,
                lhs.id,
                lhs.x
            FROM
                timeseries AS lhs
            JOIN
                (
                    SELECT
                        name AS max_name,
                        MAX(x) AS max_x
                    FROM timeseries
                    GROUP BY name
                ) AS rhs
            ON
                lhs.name = rhs.max_name AND
                lhs.x = rhs.max_x
        """
        )

        # should not fail
        df = result.compute()

        self.assertGreater(len(df), 0)
