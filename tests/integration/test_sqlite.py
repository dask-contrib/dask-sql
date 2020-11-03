import pytest
import sqlite3


@pytest.fixture(scope="session")
def engine():
    yield sqlite3.connect(":memory:")


def test_select(assert_query_gives_same_result):
    assert_query_gives_same_result(
        """
        SELECT * FROM df1
    """
    )

    assert_query_gives_same_result(
        """
        SELECT df1.user_id + 5 AS user_id, 2 * df1.b AS b FROM df1
    """
    )

    assert_query_gives_same_result(
        """
        SELECT df2.user_id, df2.d FROM df2
    """
    )

    assert_query_gives_same_result(
        """
        SELECT 1 AS I, -5.34344 AS F, 'öäll' AS S
    """
    )

    assert_query_gives_same_result(
        """
        SELECT CASE WHEN user_id = 3 THEN 4 ELSE user_id END FROM df2
    """
    )


def test_join(assert_query_gives_same_result):
    assert_query_gives_same_result(
        """
        SELECT
            df1.user_id, df1.a, df1.b,
            df2.user_id AS user_id_2, df2.c, df2.d
        FROM df1
        JOIN df2 ON df1.user_id = df2.user_id
    """,
        ["user_id", "a", "b", "user_id_2", "c", "d"],
    )


def test_sort(assert_query_gives_same_result):
    assert_query_gives_same_result(
        """
        SELECT
            user_id, b
        FROM df1
        ORDER BY b, user_id DESC
    """
    )

    assert_query_gives_same_result(
        """
        SELECT
            c, d
        FROM df2
        WHERE d IS NOT NULL -- sqlite sorts the NaNs in a strange way
        ORDER BY c, d, user_id
    """
    )


def test_limit(assert_query_gives_same_result):
    assert_query_gives_same_result(
        """
        SELECT
            c, d
        FROM df2
        WHERE d IS NOT NULL -- sqlite sorts the NaNs in a strange way
        ORDER BY c, d, user_id
        LIMIT 10 OFFSET 20
    """
    )

    assert_query_gives_same_result(
        """
        SELECT
            c, d
        FROM df2
        WHERE d IS NOT NULL -- sqlite sorts the NaNs in a strange way
        ORDER BY c, d, user_id
        LIMIT 200
    """
    )


def test_groupby(assert_query_gives_same_result):
    assert_query_gives_same_result(
        """
        SELECT
            d, SUM(c), SUM(user_id)
        FROM df2
        WHERE d IS NOT NULL -- dask behaves differently on NaNs in groupbys
        GROUP BY d
        ORDER BY SUM(c)
        LIMIT 10
    """
    )


def test_filter(assert_query_gives_same_result):
    assert_query_gives_same_result(
        """
        SELECT
            a
        FROM df1
        WHERE
            user_id = 3 AND a > 0.5
    """
    )

    assert_query_gives_same_result(
        """
        SELECT
            d
        FROM df2
        WHERE
            d NOT LIKE '%c'
    """
    )
