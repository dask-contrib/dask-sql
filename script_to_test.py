import dask.dataframe as dd
import pandas as pd

from dask_sql.context import Context

df = dd.from_pandas(pd.DataFrame({"a": [1, 2, 3], "b": [1.1, 2.2, 3.3]}), npartitions=1)

c = Context()
c.register_dask_table(df, "my_table")

df = c.sql("SELECT a, b from my_table WHERE a > 1 AND b < 3")

print(df.compute())