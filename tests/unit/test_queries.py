import os

import pytest

QUERIES = [
    f"q{i}a.sql" if i in {14, 23, 24, 39} else f"q{i}.sql" for i in range(1, 100)
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
