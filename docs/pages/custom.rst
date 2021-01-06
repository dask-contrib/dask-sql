.. _custom:

Custom Functions and Aggregations
=================================

Additional to the included SQL functionalities, it is possible to include custom functions and aggregations into the SQL queries of ``dask-sql``.
The custom functions are classified into scalar functions and aggregations.
If you want to combine Machine Learning with SQL, you might also be interested in :ref:`machine_learning`.

Scalar Functions
----------------

A scalar function (such as :math:`x \to x^2`) turns a given column into another column of the same length.
It can be registered for usage in SQL with the :func:`dask_sql.Context.register_function` method.

Example:

.. code-block:: python

    def f(x):
        return x ** 2

    c.register_function(f, "f", [("x", np.int64)], np.int64)

The registration gives a name to the function and also adds type information on the input types and names, as well as the return type.
All usual numpy types (e.g. ``np.int64``) and pandas types (``Int64``) are supported.

After registration, the function can be used as any other usual SQL function:

.. code-block:: python

    c.sql("SELECT f(column) FROM data")

Scalar functions can have one or more input parameters and can combine columns and literal values.

Aggregation Functions
---------------------

Aggregation functions run on a single column and turn them into a single value.
This means they can only be used in ``GROUP BY`` aggregations.
They can be registered with the :func:`dask_sql.Context.register_aggregation` method.
This time however, an instance of a :class:`dask.dataframe.Aggregation` needs to be passed
instead of a plain function.
More information on dask aggregations can be found in the
`dask documentation <https://docs.dask.org/en/latest/dataframe-groupby.html#aggregate>`_.

Example:

.. code-block:: python

    my_sum = dd.Aggregation("my_sum", lambda x: x.sum(), lambda x: x.sum())
    c.register_aggregation(my_sum, "my_sum", [("x", np.float64)], np.float64)

    c.sql("SELECT my_sum(other_colum) FROM df GROUP BY column")

.. note::

    There can only ever exist a single function with the same name.
    No matter if this is an aggregation function or a scalar function.