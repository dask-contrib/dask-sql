import os

import pytest

XFAIL_QUERIES = (
    1,
    2,
    4,
    5,
    6,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    16,
    17,
    18,
    20,
    21,
    22,
    23,
    24,
    27,
    28,
    30,
    32,
    34,
    35,
    36,
    37,
    39,
    40,
    41,
    43,
    44,
    45,
    47,
    48,
    49,
    50,
    51,
    53,
    54,
    57,
    58,
    59,
    62,
    63,
    64,
    66,
    67,
    69,
    70,
    72,
    73,
    74,
    75,
    76,
    77,
    78,
    80,
    81,
    82,
    83,
    84,
    85,
    86,
    87,
    88,
    89,
    92,
    94,
    95,
    97,
    98,
    99,
)

QUERIES = [
    pytest.param(f"q{i}a.sql", marks=pytest.mark.xfail if i in XFAIL_QUERIES else None)
    if i in {14, 23, 24, 39}
    else pytest.param(
        f"q{i}.sql", marks=pytest.mark.xfail if i in XFAIL_QUERIES else None
    )
    for i in range(1, 100)
]


@pytest.fixture(scope="module")
def c():
    # Lazy import, otherwise the pytest framework has problems
    from dask_sql.context import Context

    c = Context()
    for table_name in os.listdir(f"{os.path.dirname(__file__)}/data/"):
        c.create_table(
            table_name,
            f"{os.path.dirname(__file__)}/data/{table_name}",
            format="parquet",
            gpu=False,
        )

    yield c


@pytest.fixture(scope="module")
def gpu_c():
    pytest.importorskip("dask_cudf")

    # Lazy import, otherwise the pytest framework has problems
    from dask_sql.context import Context

    c = Context()
    for table_name in os.listdir(f"{os.path.dirname(__file__)}/data/"):
        c.create_table(
            table_name,
            f"{os.path.dirname(__file__)}/data/{table_name}",
            format="parquet",
            gpu=True,
        )

    yield c


@pytest.mark.queries
@pytest.mark.parametrize(
    "context,client",
    [("c", "client"), pytest.param("gpu_c", "gpu_client", marks=pytest.mark.gpu)],
)
@pytest.mark.parametrize("query", QUERIES)
def test_query(context, client, query, request):
    c = request.getfixturevalue(context)
    client = request.getfixturevalue(client)

    with open(f"{os.path.dirname(__file__)}/queries/{query}") as f:
        sql = f.read()

    res = c.sql(sql)
    res.compute(scheduler=client)
