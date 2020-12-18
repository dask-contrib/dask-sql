import pytest

# skip the test if the docker package is not installed
docker = pytest.importorskip("docker")
sqlalchemy = pytest.importorskip("sqlalchemy")


@pytest.fixture(scope="session")
def engine():
    client = docker.from_env()

    network = client.networks.create("dask-sql", driver="bridge")
    postgres = client.containers.run(
        "postgres:latest",
        detach=True,
        remove=True,
        network="dask-sql",
        environment={"POSTGRES_HOST_AUTH_METHOD": "trust"},
    )

    try:
        # Wait for it to start
        start_counter = 2
        postgres.exec_run(["bash"])
        for l in postgres.logs(stream=True):
            if b"database system is ready to accept connections" in l:
                start_counter -= 1

            if start_counter == 0:
                break

        # get the address and create the connection
        postgres.reload()
        address = postgres.attrs["NetworkSettings"]["Networks"]["dask-sql"]["IPAddress"]
        port = 5432

        engine = sqlalchemy.create_engine(
            f"postgresql+psycopg2://postgres@{address}:{port}/postgres"
        )
        yield engine
    except:
        postgres.kill()
        network.remove()

        raise

    postgres.kill()
    network.remove()


def test_select(assert_query_gives_same_result):
    assert_query_gives_same_result(
        """
        SELECT * FROM df1
    """
    )

    assert_query_gives_same_result(
        """
        SELECT
            df1.user_id + 5,
            2 * df1.a + df1.b / df1.user_id - df1.b,
            df1.a IS NULL,
            df1.a IS NOT NULL,
            df1.b_bool IS TRUE,
            df1.b_bool IS NOT TRUE,
            df1.b_bool IS FALSE,
            df1.b_bool IS NOT FALSE,
            df1.b_bool IS UNKNOWN,
            df1.b_bool IS NOT UNKNOWN,
            ABS(df1.a),
            ACOS(df1.a),
            ASIN(df1.a),
            ATAN(df1.a),
            ATAN2(df1.a, df1.b),
            CBRT(df1.a),
            CEIL(df1.a),
            COS(df1.a),
            COT(df1.a),
            DEGREES(df1.a),
            EXP(df1.a),
            FLOOR(df1.a),
            LOG10(df1.a),
            LN(df1.a),
            POWER(df1.a, 3),
            POWER(df1.a, -3),
            POWER(df1.a, 1.1),
            RADIANS(df1.a),
            ROUND(df1.a),
            SIGN(df1.a),
            SIN(df1.a),
            TAN(df1.a)
        FROM df1
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
        SELECT CASE WHEN user_id <> 3 THEN 4 ELSE 2 END FROM df2
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
    # TODO: this test fails, as NaNs are sorted differently
    # in pandas and postgresql
    # assert_query_gives_same_result(
    #     """
    #     SELECT
    #         user_id, b
    #     FROM df1
    #     ORDER BY b, user_id DESC
    # """
    # )

    assert_query_gives_same_result(
        """
        SELECT
            c, d
        FROM df2
        ORDER BY c, d, user_id
    """
    )


def test_limit(assert_query_gives_same_result):
    assert_query_gives_same_result(
        """
        SELECT
            c, d
        FROM df2
        ORDER BY c, d, user_id
        LIMIT 10 OFFSET 20
    """
    )

    assert_query_gives_same_result(
        """
        SELECT
            c, d
        FROM df2
        ORDER BY c, d, user_id
        LIMIT 200
    """
    )


def test_groupby(assert_query_gives_same_result):
    assert_query_gives_same_result(
        """
        SELECT
            d, SUM(1.0 * c), AVG(1.0 * user_id)
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

    assert_query_gives_same_result(
        """
        SELECT
            d
        FROM df2
        WHERE
            (d NOT LIKE '%c') IS NULL
    """
    )


def test_string_operations(assert_query_gives_same_result):
    assert_query_gives_same_result(
        """
        SELECT
            s,
            s || 'hello' || s,
            s SIMILAR TO '%(b|d)%',
            s SIMILAR TO '%(B|c)%',
            s SIMILAR TO '%[a-zA-Z]%',
            s SIMILAR TO '.*',
            s LIKE '%(b|d)%',
            s LIKE '%(B|c)%',
            s LIKE '%[a-zA-Z]%',
            s LIKE '.*',
            CHAR_LENGTH(s),
            UPPER(s),
            LOWER(s),
            POSITION('a' IN s),
            POSITION('ZL' IN s),
            TRIM('a' FROM s),
            TRIM(BOTH 'a' FROM s),
            TRIM(LEADING 'a' FROM s),
            TRIM(TRAILING 'a' FROM s),
            OVERLAY(s PLACING 'XXX' FROM 2),
            OVERLAY(s PLACING 'XXX' FROM 2 FOR 4),
            OVERLAY(s PLACING 'XXX' FROM 2 FOR 1),
            SUBSTRING(s FROM -1),
            SUBSTRING(s FROM 10),
            SUBSTRING(s FROM 2),
            SUBSTRING(s FROM 2 FOR 2),
            INITCAP(s),
            INITCAP(UPPER(s)),
            INITCAP(LOWER(s))
        FROM df3
    """
    )
