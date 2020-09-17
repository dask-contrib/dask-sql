How does it work?
=================

At the core, ``dask-sql`` does two things:

- translate the SQL query using `Apache Calcite <https://calcite.apache.org/>`_ into a relational algebra,
  which is specified as a tree of java objects - similar to many other SQL engines (Hive, Flink, ...)
- convert this description of the query from java objects into dask API calls (and execute them) - returning a dask dataframe.

For the first step, Apache Calcite needs to know about the columns and types of the dask dataframes,
therefore some java classes to store this information for dask dataframes are defined in ``planner``.
After the translation to a relational algebra is done (using ``RelationalAlgebraGenerator.getRelationalAlgebra``),
the python methods defined in ``dask_sql.physical`` turn this into a physical dask execution plan by converting
each piece of the relational algebra one-by-one.
