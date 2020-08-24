dask-sql
========

Installation
------------

Create a new conda environment using the supplied `conda.yaml`:

    conda create -p conda-env --file conda.yaml -c conda-forge
    conda activate conda-env/

Now run the java build

    python setup.py java

Finally, you can install the python module in development mode

    pip install -e .

Testing
-------

Currently, there is only a small amount of tests available. You can run them with

    pytest tests

After installation, you can also use it in your jupyter notebooks or other applications. For example

```python
import dask.dataframe as dd
import pandas as pd

from dask_sql.context import Context

df = dd.from_pandas(
        pd.DataFrame({"a": [0]*100 + [1]*100 + [2]*100}),
    npartitions=3)

c = Context()
c.register_dask_table(df, "my_table")

df = c.sql("SELECT SUM(a) FROM my_table")
# df is a dask dataframe now
print(df.compute())
```

You can also test the sql server by running

    python dask_sql/server/handler.py

in one terminal and

    psql -h localhost -p 9876

in another. You can use it to fire simple SQL queries (as no data is loaded by default):

    => SELECT 1 + 1;
     EXPR$0
    --------
        2
    (1 row)

