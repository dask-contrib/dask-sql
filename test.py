import pandas as pd
from dask.distributed import Client
from dask_cuda import LocalCUDACluster

from dask_sql import Context

if __name__ == "__main__":
    cluster = LocalCUDACluster(protocol="ucx")
    client = Client(cluster)

    c = Context()

    test_df = pd.DataFrame({"id": ["0", "1", "2"]})
    c.create_table("test", test_df)

    # segfault
    result = c.sql("select id from test").compute()
    print(result)


# from dask_planner import rust
# from dask_planner.rust import Statement, sql_functions

# print(f'ok: {Statement.table_name()}')
# sql = "select id from test"
# print(f'SQL: {sql}')
# state = Statement.sql(sql)
# print(f'Result: {state}')

# query = sql_functions.query(state)
# print(f'Query: {query}')

# select = sql_functions.select(query)
# print(f'SELECT : {dir(select)}')

# import pandas as pd

# from dask_sql import Context

# c = Context()
# test_df = pd.DataFrame({"id": ["0", "1", "2"]})
# c.create_table("test", test_df)
# result = c.sql("select id from test").compute()
# print(result)
