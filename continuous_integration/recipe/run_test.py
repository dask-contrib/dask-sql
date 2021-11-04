import dask.dataframe as dd
import pandas as pd

from dask_sql import Context

c = Context()

data = """
name,x
Alice,34
Bob,
"""

df = pd.DataFrame({"name": ["Alice", "Bob", "Chris"] * 100, "x": list(range(300))})
ddf = dd.from_pandas(df, npartitions=10)
c.create_table("my_data", ddf)

got = c.sql(
    """
    SELECT
        my_data.name,
        SUM(my_data.x) AS "S"
    FROM
        my_data
    GROUP BY
        my_data.name
"""
)
expect = pd.DataFrame({"name": ["Alice", "Bob", "Chris"], "S": [14850, 14950, 15050]})

dd.assert_eq(got, expect)
