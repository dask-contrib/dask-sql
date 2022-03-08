from dask.distributed import Client
from dask_cuda import LocalCUDACluster
from dask_sql import Context
import pandas as pd


if __name__ == "__main__":
    cluster = LocalCUDACluster(protocol="ucx")
    client = Client(cluster)

    c = Context()

    test_df = pd.DataFrame({'id': [0, 1, 2]})
    c.create_table("test", test_df)

    # segfault
    c.sql("select count(*) from test")
