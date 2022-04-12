import logging

import pandas as pd

from dask_sql import Context

# from dask.distributed import Client
# from dask_cuda import LocalCUDACluster


# if __name__ == "__main__":
#     cluster = LocalCUDACluster(protocol="ucx")
#     client = Client(cluster)

#     c = Context()

#     test_df = pd.DataFrame({"id": ["0", "1", "2"]})
#     c.create_table("test", test_df)

#     # segfault
#     result = c.sql("select count(*) from test").compute()
#     print(result)


c = Context(logging_level=logging.DEBUG)

test_df = pd.DataFrame({"id": [0, 1, 2]})
c.create_table("test", test_df)
result = c.sql("select id from test").compute()
print(result)

print("done")
