# dask-sql

`dask-sql` adds a SQL query layer on top of `dask`.

It lets you register your dask dataframes and then run SQL queries against them.
The queries will run as normal dask operations, which can be distributed within your dask cluster.
The goal is therefore similar to what Spark SQL/Hive/Drill/... is for the Hadoop world - but with much less features (so far...)

It uses [Apache Calcite](https://calcite.apache.org/) to convert
SQL queries into a query plan (so called relational algebra) - similar to many other SQL engines (Hive, Flink, ...).
This plan is then converted into normal dask API calls.
Some ideas for this project are coming from the [blazingSQL](https://github.com/BlazingDB/blazingsql) project.


`dask-sql` is currently under development. Any contributions are highly welcome!
It is mainly a proof of concept - many things are still missing.

Example
-------

We use the NYC flight data from the [dask tutorial](https://github.com/dask/dask-tutorial/blob/master/04_dataframe.ipynb), as an example:

```python
from dask_sql import Context
import dask.dataframe as dd

# Create a context to hold the registered tables
c = Context()

# Load the data and register it in the context
# This will give the table a name
df = dd.read_csv(...)
c.register_dask_table(df, "nycflights")

# Now execute an SQL query. The result is a dask dataframe
result = c.sql("""
    SELECT
        Origin, MAX(DepDelay)
    FROM nycflights
    GROUP BY Origin
""")

# Show the result (or use it for any other dask calculation)
print(result.compute())
```

`dask-sql` also comes with a very simple test implementation of a SQL server speaking the [postgreSQL wire protocol](https://www.postgresql.org/docs/9.3/protocol-flow.html).
It is - so far - just a proof of concept. See below on how to start it.

## Installation

So far, the project can only be installed via the sources.
A packaged installation will come soon!

### With `conda`

Create a new conda environment using the supplied `conda.yaml`:

    conda create -n dask-sql --file conda.yaml -c conda-forge
    conda activate dask-sql

Finally, you can install the python module

    python setup.py install

This will trigger also the compilation of the java library.

### With `pip`

Make sure you have a running java installation with version >= 11.
Currently, for the compilation the JDK is needed.

After installing Java, you can install the package with

    python setup.py install

### For development

You can also build the java library separately:

    python setup.py java

After that, you can install the package in development mode

    pip install -e .

Make sure to re-run the java build whenever you do any changes to the
java code.

## Testing

You can run the tests (after installation) with

    pytest tests

## SQL Server

`dask-sql` comes with a small test implementation for a SQL server.
Instead of rebuilding a full ODBC driver, we re-use the [postgreSQL wire protocol](https://www.postgresql.org/docs/9.3/protocol-flow.html).

You can test the sql postgres server by running

    python dask_sql/server/handler.py

in one terminal. This will spin up a server on port 9876
that looks similar to a normal postgres database to any postgres client
(except that you can only do queries, no database creation etc.)

You can test this for example with the default postgres client:

    psql -h localhost -p 9876

Now you can fire simple SQL queries (as no data is loaded by default):

    => SELECT 1 + 1;
     EXPR$0
    --------
        2
    (1 row)

## How does it work?

At the core, `dask-sql` does two things:
* translate the SQL query using Apache Calcite into a relational algebra, which is specified as a tree of java objects.
* convert this description of the query from java objects into dask API calls (and execute them) - returning a dask dataframe.

For the first step, Apache Calcite needs to know about the columns and types of the dask dataframes, therefore some java classes to store this information for dask dataframes are defined in `planner`.
After the translation to a relational algebra is done (using `RelationalAlgebraGenerator.getRelationalAlgebra`), the python methods defined in `dask_sql.physical` turn this into a physical dask execution plan by converting each piece of the relational algebra one-by-one.
