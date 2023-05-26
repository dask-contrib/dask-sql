import os

import pytest

XFAIL_QUERIES = (
    5,
    6,
    8,
    9,
    10,
    14,
    16,
    18,
    22,
    23,
    24,
    27,
    28,
    35,
    36,
    39,
    41,
    44,
    45,
    47,
    49,
    51,
    53,
    54,
    57,
    58,
    62,
    63,
    67,
    69,
    70,
    72,
    75,
    77,
    80,
    86,
    87,
    88,
    89,
    92,
    94,
    95,
    99,
)

QUERIES = [
    pytest.param(f"q{i}.sql", marks=pytest.mark.xfail if i in XFAIL_QUERIES else ())
    for i in range(1, 100)
]


@pytest.fixture(scope="module")
def c(filepath):
    # Lazy import, otherwise the pytest framework has problems
    from dask_sql.context import Context

    c = Context()
    if not filepath:
        filepath = f"{os.path.dirname(__file__)}/data/"
    for table_name in os.listdir(filepath):
        c.create_table(
            table_name,
            filepath + "/" + table_name,
            format="parquet",
            gpu=False,
        )

    yield c


@pytest.fixture(scope="module")
def gpu_c(filepath):
    pytest.importorskip("dask_cudf")

    # Lazy import, otherwise the pytest framework has problems
    from dask_sql.context import Context

    c = Context()
    if not filepath:
        filepath = f"{os.path.dirname(__file__)}/data/"
    for table_name in os.listdir(filepath):
        c.create_table(
            table_name,
            filepath + "/" + table_name,
            format="parquet",
            gpu=True,
        )

    yield c


@pytest.mark.queries
@pytest.mark.parametrize("query", QUERIES)
def test_query(c, client, query):
    with open(f"{os.path.dirname(__file__)}/queries/{query}") as f:
        sql = f.read()

    res = c.sql(sql)
    res.compute(scheduler=client)


@pytest.mark.gpu
@pytest.mark.queries
@pytest.mark.parametrize("query", QUERIES)
def test_gpu_query(gpu_c, gpu_client, query):
    with open(f"{os.path.dirname(__file__)}/queries/{query}") as f:
        sql = f.read()

    res = gpu_c.sql(sql)
    res.compute(scheduler=gpu_client)
