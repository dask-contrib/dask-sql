# dask-sql

[![Conda](https://img.shields.io/conda/v/conda-forge/dask-sql)](https://anaconda.org/conda-forge/dask-sql)
[![PyPI](https://img.shields.io/pypi/v/dask-sql?logo=pypi)](https://pypi.python.org/pypi/dask-sql/)
[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/nils-braun/dask-sql/Test?logo=github)](https://github.com/nils-braun/dask-sql/actions)
[![Read the Docs](https://img.shields.io/readthedocs/dask-sql)](https://dask-sql.readthedocs.io/en/latest/)
[![Codecov](https://img.shields.io/codecov/c/github/nils-braun/dask-sql?logo=codecov)](https://codecov.io/gh/nils-braun/dask-sql)
[![GitHub](https://img.shields.io/github/license/nils-braun/dask-sql)](https://github.com/nils-braun/dask-sql/blob/main/LICENSE.txt)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/nils-braun/dask-sql-binder/main?urlpath=lab)

`dask-sql` adds a SQL query layer on top of `pandas` and `Dask`.
This allows you to query and transform your data frames using
common SQL operations and also simply scale up the calculation if you need it.

![SQL + Python](./.github/heart.png)

The goal of this project is similar to what Spark SQL/Hive/Drill/... is for the Hadoop world - but with a tight integration with Python.
Some ideas for this project are coming from the very great [blazingSQL](https://github.com/BlazingDB/blazingsql) project.

Read more in the [documentation](https://dask-sql.readthedocs.io/en/latest/).


![dask-sql GIF](./.github/animation.gif)

---

**NOTE**

`dask-sql` is currently under development and does so far not understand all SQL commands.
We are actively looking for feedback, improvements and contributors!

---

## Example

We use the timeseries random data from `dask.datasets` as an example.
Any pandas or Dask data frame will work.

```python
from dask_sql import Context
from dask.datasets import timeseries

# Create a context to hold the registered tables
c = Context()

# Load the data and register it in the context
# This will give the table a name
df = timeseries()
c.create_table("timeseries", df)

# Now execute an SQL query. The result is a dask dataframe
# The query looks for the id with the highest x for each name
# (this is just random test data, but you could think of looking
# for outliers in the sensor data)
result = c.sql("""
    SELECT
        lhs.name,
        lhs.id,
        lhs.x
    FROM
        timeseries AS lhs
    JOIN
        (
            SELECT
                name AS max_name,
                MAX(x) AS max_x
            FROM timeseries
            GROUP BY name
        ) AS rhs
    ON
        lhs.name = rhs.max_name AND
        lhs.x = rhs.max_x
""", return_futures=False)

# Show the result
print(result)
```

You can also run the CLI `dask-sql` for testing out SQL commands quickly:

    dask-sql --load-test-data --startup

    (dask-sql) > SELECT * FROM timeseries LIMIT 10;

## Installation

`dask-sql` can be installed via `conda` (preferred) or `pip` - or in a development environment.

### With `conda`

Create a new conda environment or use your already present environment:

    conda create -n dask-sql
    conda activate dask-sql

Install the package from the `conda-forge` channel:

    conda install dask-sql -c conda-forge

### With `pip`

`dask-sql` needs Java for the parsing of the SQL queries.
Make sure you have a running java installation with version >= 8.

To test if you have Java properly installed and set up, run

    $ java -version
    openjdk version "1.8.0_152-release"
    OpenJDK Runtime Environment (build 1.8.0_152-release-1056-b12)
    OpenJDK 64-Bit Server VM (build 25.152-b12, mixed mode)

After installing Java, you can install the package with

    pip install dask-sql

### For development

If you want to have the newest (unreleased) `dask-sql` version or if you plan to do development on `dask-sql`, you can also install the package from sources.

    git clone https://github.com/nils-braun/dask-sql.git

Create a new conda environment and install the development environment:

    conda create -n dask-sql --file conda.txt -c conda-forge

It is not recommended to use `pip` instead of `conda` for the environment setup.
If you however need to, make sure to have Java (jdk >= 8) and maven installed and correctly setup before continuing.
Have a look into `conda.txt` for the rest of the development environment.

After that, you can install the package in development mode

    pip install -e ".[dev]"

To compile the Java classes (at the beginning or after changes), run

    python setup.py java

This repository uses [pre-commit](https://pre-commit.com/) hooks. To install them, call

    pre-commit install

## Testing

You can run the tests (after installation) with

    pytest tests

## How does it work?

At the core, `dask-sql` does two things:

- translate the SQL query using [Apache Calcite](https://calcite.apache.org/) into a relational algebra, which is specified as a tree of java objects - similar to many other SQL engines (Hive, Flink, ...)
- convert this description of the query from java objects into dask API calls (and execute them) - returning a dask dataframe.

For the first step, Apache Calcite needs to know about the columns and types of the dask dataframes, therefore some java classes to store this information for dask dataframes are defined in `planner`.
After the translation to a relational algebra is done (using `RelationalAlgebraGenerator.getRelationalAlgebra`), the python methods defined in `dask_sql.physical` turn this into a physical dask execution plan by converting each piece of the relational algebra one-by-one.

## SQL Server

`dask-sql` comes with a small test implementation for a SQL server.
Instead of rebuilding a full ODBC driver, we re-use the [presto wire protocol](https://github.com/prestodb/presto/wiki/HTTP-Protocol).
It is - so far - only a start of the development and missing important concepts, such as
authentication.

You can test the sql presto server by running (after installation)

    dask-sql-server

or by using the created docker image

    docker run --rm -it -p 8080:8080 nbraun/dask-sql

in one terminal. This will spin up a server on port 8080 (by default)
that looks similar to a normal presto database to any presto client.

You can test this for example with the default [presto client](https://prestosql.io/docs/current/installation/cli.html):

    presto --server localhost:8080

Now you can fire simple SQL queries (as no data is loaded by default):

    => SELECT 1 + 1;
     EXPR$0
    --------
        2
    (1 row)

You can find more information in the [documentation](https://dask-sql.readthedocs.io/en/latest/pages/server.html).
