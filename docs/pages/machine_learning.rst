.. _machine_learning:

Machine Learning
================

.. note::

    Machine Learning support is experimental in ``dask-sql``.
    We encourage you to try it out and report any issues on our
    `issue tracker <https://github.com/nils-braun/dask-sql/issues>`_.

Both the training as well as the prediction using Machine Learning methods play a crucial role in
many data analytics applications. ``dask-sql`` supports Machine Learning
applications in different ways, depending on how much you would like to do in Python or SQL.

Please also see :ref:`ml` for more information on the SQL statements used on this page.

1. Data Preparation in SQL, Training and Prediction in Python
-------------------------------------------------------------

If you are familiar with Python and the ML ecosystem in Python, this one is probably
the simplest possibility. You can use the :func:`~dask_sql.Context.sql` call as described
before to extract the data for your training or ML prediction.
The result will be a Dask dataframe, which you can either directly feed into your model
or convert to a pandas dataframe with `.compute()` before.

This gives you full control on the training process and the simplicity of
using SQL for data manipulation. You can use this method in your python scripts
or Jupyter notebooks, but not from the :ref:`server` or :ref:`cmd`.

2. Training in Python, Prediction in SQL
----------------------------------------

In many companies/teams, it is typical that some team members are responsible for
creating/training a ML model, and others use it to predict unseen data.
It would be possible to create a custom function (see :ref:`custom`) to load and use the model,
which then can be used in ``SELECT`` queries.
However for convenience, ``dask-sql`` introduces a SQL keyword to do this work for you
automatically. The syntax is similar to the `BigQuery Predict Syntax <https://cloud.google.com/bigquery-ml/docs/reference/standard-sql/bigqueryml-syntax-predict>`_.

.. code-block:: python

    c.sql("""
    SELECT * FROM PREDICT ( MODEL my_model,
        SELECT x, y, z FROM data
    )
    """)

This call will first collect the data from the inner ``SELECT`` call (which can be any valid
``SELECT`` call, including ``JOIN``, ``WHERE``, ``GROUP BY``, custom tables and views etc.)
and will then apply the model with the name "my_model" for prediction.
The model needs to be registered at the context before using :func:`~dask_sql.Context.register_model`.

.. code-block:: python

    c.register_model("my_model", model)

The model registered here can be any valid python object, which follows the scikit-learn
interface, which is to have a ``predict()`` function.
Please note that the input will not be pandas dataframe, but a Dask dataframe.
See :ref:`ml` for more information.

3. Training and Prediction in SQL
---------------------------------

This method, in contrast to the other two possibilities, works completely from SQL,
which allows you to also call it e.g. from your BI tool.
Additionally to the ``PREDICT`` keyword mentioned above, ``dask-sql`` also has a way to
create and train a model from SQL:

.. code-block:: sql

    CREATE MODEL my_model WITH (
        model_class = 'sklearn.ensemble.GradientBoostingClassifier',
        wrap_predict = True,
        target_column = 'target'
    ) AS (
        SELECT x, y, target
        FROM timeseries
        LIMIT 100
    )

This call will create a new instance of ``sklearn.ensemble.GradientBoostingClassifier``
and train it with the data collected from the ``SELECT`` call (again, every valid ``SELECT``
query can be given). The model can than be used in subsequent calls to ``PREDICT``
using the given name.
Have a look into :ref:`ml` for more information.

4. Check Model parameters - Model meta data
-------------------------------------------
After the model was trained, you can inspect and get model details by using the
following sql statements

.. code-block:: sql

    -- show the list of models  which are trained and stored in the context.
    SHOW MODELS

    -- To get the hyperparameters of the trained MODEL, use
    -- DESCRIBE MODEL <model_name>.
    DESCRIBE MODEL my_model


5. Export Trained Model
------------------------
Once your model was trained and performs good in your validation dataset,
you can export the model into a file with one of the supported model serialization
formats like pickle, joblib, mlflow (framework-agnostic serialization format), etc.

Currently, dask-sql supports the pickle, joblib and mlflow format for exporting the
trained model, which can then be deployed as microservices etc

Before training and exporting the models from different framework like
lightgbm, catboost, please ensure the relevant packages are installed in the
dask-sql environment, otherwise it will raise an exception on import and if you
are using mlflow as format ensure mlflow was installed


.. code-block:: sql

    -- for pickle model serialization
    EXPORT MODEL my_model WITH (
        format ='pickle',
        location = 'model.pkl'
    )

    -- for joblib model serialization
    EXPORT MODEL my_model WITH (
        format ='joblib',
        location = 'model.pkl'
    )

    -- for mlflow model serialization
    EXPORT MODEL my_model WITH (
        format ='mlflow',
        location = 'mlflow_dir'
    )

    -- Note you can pass more number of key value pairs
    -- (parameters) which will be delegated to the respective
    -- export functions


Example
~~~~~~~

The following SQL-only code gives an example on how the commands can play together.
We assume that you have created/registered a table "my_data" with the numerical columns ``x`` and ``y``
and the boolean target ``label``.

.. code-block:: sql

    -- First, we create a new feature z out of x and y.
    -- For convenience, we store it in another table
    CREATE OR REPLACE TABLE transformed_data AS (
        SELECT x, y, x + y AS z, label
        FROM my_data
    )

    -- We split the data into a training set
    -- by using the first 100 items.
    -- Please note that this is just for a very quick-and-dirty
    -- example - you would probably want to do something
    -- more advanced here, maybe with TABLESAMPLE
    CREATE OR REPLACE TABLE training_data AS (
        SELECT * FROM transformed_data
        LIMIT 15
    )

    -- Quickly check the data
    SELECT * FROM training_data

    -- We can now train a model from the sklearn package.
    -- Make sure to install it together with dask-ml with conda or pip.
    CREATE OR REPLACE MODEL my_model WITH (
        model_class = 'sklearn.ensemble.GradientBoostingClassifier',
        wrap_predict = True,
        target_column = 'label'
    ) AS (
        SELECT * FROM training_data
    )

    -- Now apply the trained model on all the data
    -- and compare.
    SELECT
        *, (CASE WHEN target = label THEN True ELSE False END) AS correct
    FROM PREDICT(MODEL my_model,
        SELECT * FROM transformed_data
    )
    -- list models
    SHOW MODELS
    -- check parameters of the model
    DESCRIBE MODEL my_model

    -- export model
    EXPORT MODEL my_model WITH (
        format ='pickle',
        location = 'model.pkl'
    )
