import dask.dataframe as dd
import pandas as pd

from context import add_dask_table, get_ral, apply_ral

df = dd.from_pandas(pd.DataFrame({"a": [1, 2, 3], "b": [1.1, 2.2, 3.3]}), npartitions=1)

add_dask_table(df, "my_table")

ral = get_ral("SELECT a, b from my_table WHERE a > 1 AND b < 3")
df = apply_ral(ral)

print(df.compute())