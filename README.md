dask-sql
========

`dask-sql` adds a SQL query layer on top of `dask`.
It lets you register your dask dataframes and then run SQL queries against it.
It uses [Apache Calcite](https://calcite.apache.org/) to convert
SQL queries into a query plan (so called relational algebra) - similar to many other SQL engines (Hive, Flink or blazingSQL, where this project borrowed some ideas).
This plan is then converted into normal dask API calls.

For example (using the NYC flight data from the [dask tutorial](https://github.com/dask/dask-tutorial/blob/master/04_dataframe.ipynb)):

```python
from dask_sql.context import Context

# Create a context to hold the registered tables
c = Context()

# Load the data and register it in the context
# Please note: so far there is no support for date time types,
# so you need to remove the date column
df = ...
del df["Date"]
c.register_dask_table(df, "nycflights")

# Now execute an SQL query. The result is a dask dataframe
result = c.sql("""
    SELECT
        Origin, MAX(DepDelay)
    FROM nycflights
    GROUP BY Origin
""")

# Show the result
print(result.compute())
```

`dask-sql` also comes with a very simple test implementation of a SQL server speaking the [postgreSQL wire protocol](https://www.postgresql.org/docs/9.3/protocol-flow.html).
It is - so far - just a proof of concept. See below on how to start it.

`dask-sql` is currently under development. Any contributions are highly welcome!

Installation
------------

Create a new conda environment using the supplied `conda.yaml`:

    conda create -p conda-env --file conda.yaml -c conda-forge
    conda activate conda-env/

Now run the java build

    python setup.py java

Finally, you can install the python module

    python setup.py install

Or also in development mode

    pip install -e .

Testing
-------

Currently, there is only a small amount of tests available. You can run them with

    pytest tests

First Steps
-----------

After installation, you can use `dask_sql` in your jupyter notebooks or other applications. For example

```python
import dask.dataframe as dd
import pandas as pd

from dask_sql.context import Context

df = dd.from_pandas(pd.DataFrame({"a": [0]*100 + [1]*100 + [2]*100}), npartitions=3)

# We first need to tell dask_sql about our table
c = Context()
c.register_dask_table(df, "my_table")

# Now we can call SQL queries against it
df = c.sql("SELECT SUM(a) FROM my_table")

# df is a dask dataframe now
print(df.compute())
```

You can also test the sql postgres server by running

    python dask_sql/server/handler.py

in one terminal and

    psql -h localhost -p 9876

in another. You can use it to fire simple SQL queries (as no data is loaded by default):

    => SELECT 1 + 1;
     EXPR$0
    --------
        2
    (1 row)

