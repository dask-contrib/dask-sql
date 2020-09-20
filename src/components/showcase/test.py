# Load the data
import dask.datasets
df = dask.datasets.timeseries()

# Register the data
from dask_sql import Context
c = Context()
c.register_dask_table(df, "timeseries")

# Query the data
result = c.sql(""" SELECT lhs.name, lhs.id, lhs.x FROM timeseries AS lhs JOIN ( SELECT name AS max_name, MAX(x) AS max_x FROM timeseries GROUP BY name ) AS rhs ON lhs.name = rhs.max_name AND lhs.x = rhs.max_x """).compute()