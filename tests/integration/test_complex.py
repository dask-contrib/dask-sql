from dask.datasets import timeseries


def test_complex_query(c):
    df = timeseries(freq="1d").persist()
    c.create_table("timeseries", df)

    result = c.sql(
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

    assert len(df) > 0
