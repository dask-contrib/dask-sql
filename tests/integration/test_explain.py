import dask.dataframe as dd
import pandas as pd
import pytest


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_sql_query_explain(c, gpu):
    df = dd.from_pandas(pd.DataFrame({"a": [1, 2, 3]}), npartitions=1)
    c.create_table("df", df, gpu=gpu)

    sql_string = c.sql("EXPLAIN SELECT * FROM df")

    assert sql_string.startswith("Projection: df.a\n")

    # TODO: Need to add statistics to Rust optimizer before this can be uncommented.
    # c.create_table("df", data_frame, statistics=Statistics(row_count=1337))

    # sql_string = c.explain("SELECT * FROM df")

    # assert sql_string.startswith(
    #     "DaskTableScan(table=[[root, df]]): rowcount = 1337.0, cumulative cost = {1337.0 rows, 1338.0 cpu, 0.0 io}, id = "
    # )

    sql_string = c.sql(
        "EXPLAIN SELECT MIN(a) AS a_min FROM other_df GROUP BY a",
        dataframes={"other_df": df},
        gpu=gpu,
    )
    assert sql_string.startswith("Projection: MIN(other_df.a) AS a_min\n")
    assert "Aggregate: groupBy=[[other_df.a]], aggr=[[MIN(other_df.a)]]" in sql_string
