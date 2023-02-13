.. _configuration:

Configuration in Dask-SQL
==========================

``dask-sql`` supports a list of configuration options to configure behavior of certain operations.
``dask-sql`` uses `Dask's config <https://docs.dask.org/en/stable/configuration.html>`_
module and configuration options can be specified with YAML files, via environment variables,
or directly, either through the `dask.config.set <https://docs.dask.org/en/stable/configuration.html#dask.config.set>`_ method
or the ``config_options`` argument in the :func:`dask_sql.Context.sql` method.

Configuration Reference
-----------------------

.. dask-config-block::
    :location: sql
    :config: https://raw.githubusercontent.com/dask-contrib/dask-sql/main/dask_sql/sql.yaml
    :schema: https://raw.githubusercontent.com/dask-contrib/dask-sql/main/dask_sql/sql-schema.yaml
