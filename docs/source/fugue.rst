FugueSQL Integrations
=====================

`FugueSQL <https://fugue-tutorials.readthedocs.io/tutorials/fugue_sql/index.html>`_ is a related project that aims to provide a unified SQL interface for a variety of different computing frameworks, including Dask.
While it offers a SQL engine with a larger set of supported commands, this comes at the cost of slower performance when using Dask in comparison to dask-sql.
In order to offer a "best of both worlds" solution, dask-sql includes several options to integrate with FugueSQL, using its faster implementation of SQL commands when possible and falling back on FugueSQL when necessary.

dask-sql as a FugueSQL engine
-----------------------------

FugueSQL users unfamiliar with dask-sql can take advantage of its functionality by installing it in an environment alongside Fugue; this will automatically register :class:`dask_sql.integrations.fugue.DaskSQLExecutionEngine` as the default Dask execution engine for FugueSQL queries.
For more information and sample usage, see `Fugue — dask-sql as a FugueSQL engine <https://fugue-tutorials.readthedocs.io/tutorials/integrations/dasksql.html>`_.

Using FugueSQL on an existing ``Context``
-----------------------------------------

dask-sql users attempting to expand their SQL querying options for an existing ``Context`` can use :func:`dask_sql.integrations.fugue.fsql_dask`, which executes the provided query using FugueSQL, using the tables within the provided context as input.
The results of this query can then optionally be registered to the context:

.. code-block:: python

    # define a custom prepartition function for FugueSQL
    def median(df: pd.DataFrame) -> pd.DataFrame:
        df["y"] = df["y"].median()
        return df.head(1)

    # create a context with some tables
    c = Context()
    ...

    # run a FugueSQL query using the context as input
    query = """
        j = SELECT df1.*, df2.x
            FROM df1 INNER JOIN df2 ON df1.key = df2.key
            PERSIST
        TAKE 5 ROWS PREPARTITION BY x PRESORT key
        PRINT
        TRANSFORM j PREPARTITION BY x USING median
        PRINT
        """
    result = fsql_dask(query, c, register=True)  # results aren't registered by default

    assert "j" in result    # returns a dict of resulting tables
    assert "j" in c.tables  # results are also registered to the context
