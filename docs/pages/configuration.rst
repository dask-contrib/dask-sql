.. _configuration:

Configuration in Dask-SQL
==========================

``dask-sql`` supports a list of configuration options to configure behavior of certain operations.
``dask-sql`` uses `Dask's config <https://docs.dask.org/en/stable/configuration.html>`_
module and configuration options can be specified with YAML files, via environment variables,
or directly through the :func:`~dask_sql.Context.set_config` or :func:`~dask_sql.Context.sql` api.

Configuration Reference
-----------------------

.. dask-config-block::
    :location: sql
    :config: https://gist.githubusercontent.com/ayushdg/1b0f7cacd0e9db20175669a17386a58d/raw/6ddb78a3b3c4ac5051aa17105e576211d0e32f6b/sql.yaml
    :schema: https://gist.githubusercontent.com/ayushdg/1b0f7cacd0e9db20175669a17386a58d/raw/2d37f64c48c2b6ebdca6634b4c5e3c22a59e1cdf/sql-schema.yaml
