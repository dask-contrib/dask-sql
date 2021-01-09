.. _ml:

Machine Learning in SQL
=======================

.. note::

    Machine Learning support is experimental in ``dask-sql``.
    We encourage you to try it out and report any issues on our
    `issue tracker <https://github.com/nils-braun/dask-sql/issues>`_.

As all SQL statements in ``dask-sql`` are eventually converted to Python calls, it is very simple to include
any custom Python function and library, e.g. Machine Learning libraries. Although it would be possible to
register custom functions (see :ref:`custom`) for this and use them, it is much more convenient if this functionality
is already included in the core SQL language.
These three statements help in training and using models. Every :class:`Context` has a registry for models, which
can be used for training or prediction.
For a full example, see :ref:`machine_learning`.

.. raw:: html

    <div class="highlight-sql notranslate">
    <div class="highlight"><pre>
    <span class="k">CREATE</span> [ <span class="k">OR REPLACE</span> ] <span class="k">MODEL</span> [ <span class="k">IF NOT EXISTS</span> ] <span class="ss">&lt;model-name></span>
        <span class="k">WITH</span> ( <span class="ss">&lt;key&gt;</span> = <span class="ss">&lt;value&gt;</span> [ , ... ] ) <span class="k">AS</span> ( <span class="k">SELECT</span> ... )
    <span class="k">DROP MODEL</span> [ <span class="k">IF EXISTS</span> ] <span class="ss">&lt;model-name></span>
    <span class="k">SELECT</span> <span class="ss">&lt;expression&gt;</span> <span class="k">FROM PREDICT</span> (<span class="k">MODEL</span> <span class="ss">&lt;model-name></span>, <span class="k">SELECT</span> ... )
    </pre></div>
    </div>

``IF [ NOT ] EXISTS`` and ``CREATE OR REPLACE`` behave similar to its analogous flags in ``CREATE TABLE``.
See :ref:`creation` for more information.

``CREATE MODEL``
----------------

Create and train a model on the data from the given SELECT query
and register it at the context.

The select query is a normal ``SELECT`` query (following the same syntax as described in :ref:`select`
or even a call to ``PREDICT`` (which typically does not make sense however) and its
result is used as the training data.

The key-value parameters control, how and which model is trained:

    * ``model_class``:
      This argument needs to be present.
      It is the full python module path to the class of the model to train.
      Any model class with sklearn interface is valid, but might or
      might not work well with Dask dataframes.
      Have a look into the
      `dask-ml documentation <https://ml.dask.org/index.html>`_
      for more information on which models work best.
      You might need to install necessary packages to use
      the models.
    * ``target_column``:
      Which column from the data to use as target.
      If not empty, it is removed automatically from
      the training data. Defaults to an empty string, in which
      case no target is feed to the model training (e.g. for
      unsupervised algorithms). This means, you typically
      want to set this parameter.
    * ``wrap_predict``:
      Boolean flag, whether to wrap the selected
      model with a :class:`dask_ml.wrappers.ParallelPostFit`.
      Have a look into the
      `dask-ml docu on ParallelPostFit <https://ml.dask.org/meta-estimators.html#parallel-prediction-and-transformation>`_
      to learn more about it. Defaults to false. Typically you set
      it to true for sklearn models if predicting on big data.
    * ``wrap_fit``:
      Boolean flag, whether to wrap the selected
      model with a :class:`dask_ml.wrappers.Incremental`.
      Have a look into the
      `dask-ml docu on Incremental <https://ml.dask.org/incremental.html>`_
      to learn more about it. Defaults to false. Typically you set
      it to true for sklearn models if training on big data.
    * ``fit_kwargs``:
      keyword arguments sent to the call to ``fit()``.

All other arguments are passed to the constructor of the
model class.

Example:

.. raw:: html

    <div class="highlight-sql notranslate"><div class="highlight"><pre><span></span><span class="k">CREATE MODEL</span> <span class="n">my_model</span> <span class="k">WITH</span> <span class="p">(</span>
        <span class="n">model_class</span> <span class="o">=</span> <span class="s1">'dask_ml.xgboost.XGBClassifier'</span><span class="p">,</span>
        <span class="n">target_column</span> <span class="o">=</span> <span class="s1">'target'</span>
    <span class="p">)</span> <span class="k">AS</span> <span class="p">(</span>
        <span class="k">SELECT</span> <span class="n">x</span><span class="p">,</span> <span class="n">y</span><span class="p">,</span> <span class="n">target</span>
        <span class="k">FROM</span> <span class="ss">"data"</span>
    <span class="p">)</span>
    </pre></div>
    </div>

This SQL call is not a 1:1 replacement for a normal
python training and can not fulfill all use-cases
or requirements!

If you are dealing with large amounts of data,
you might run into problems while model training and/or
prediction, depending if your model can cope with
dask dataframes.

    * if you are training on relatively small amounts
      of data but predicting on large data samples
      (and you are not using a model build for usage with dask
      from the dask-ml package), you might want to set
      ``wrap_predict`` to True. With this option,
      model interference will be parallelized/distributed.
    * If you are training on large amounts of data,
      you can try setting wrap_fit to True. This will
      do the same on the training step, but works only on
      those models, which have a ``fit_partial`` method.


``DROP MODEL``
--------------

Remove the model with the given name from the registered models.


``SELECT FROM PREDICT``
-----------------------

Predict the target using the given model and dataframe from the ``SELECT`` query.
The return value is the input dataframe with an additional column named
"target", which contains the predicted values.
The model needs to be registered at the context before using it in this function,
either by calling :func:`Context.register_model` explicitly or by training
a model using the ``CREATE MODEL`` SQL statement above.

A model can be anything which has a ``predict`` function.
Please note however, that it will need to act on Dask dataframes. If you
are using a model not optimized for this, it might be that you run out of memory if
your data is larger than the RAM of a single machine.
To prevent this, have a look into the dask-ml package,
especially the `ParallelPostFit <https://ml.dask.org/meta-estimators.html>`_
meta-estimator. If you are using a model trained with ``CREATE MODEL``
and the ``wrap_predict`` flag set to true, this is done automatically.

Using this SQL statement is roughly equivalent to doing

.. code-block:: python

    df = context.sql("<select query>")
    model = get the model from the context

    target = model.predict(df)
    return df.assign(target=target)

The select query is a normal ``SELECT`` query (following the same syntax as described in :ref:`select`
or even another a call to ``PREDICT``.