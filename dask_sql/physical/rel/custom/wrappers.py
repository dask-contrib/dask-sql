# Copyright 2017, Dask developers
# Dask-ML project - https://github.com/dask/dask-ml
"""Meta-estimators for parallelizing estimators using the scikit-learn API."""
import contextlib
import datetime
import logging
import warnings
from timeit import default_timer as tic

import dask.array as da
import dask.dataframe as dd
import dask.delayed
import numpy as np
import sklearn.base
import sklearn.metrics

logger = logging.getLogger(__name__)


class ParallelPostFit(sklearn.base.BaseEstimator, sklearn.base.MetaEstimatorMixin):
    """Meta-estimator for parallel predict and transform.

    Parameters
    ----------
    estimator : Estimator
        The underlying estimator that is fit.

    scoring : string or callable, optional
        A single string (see :ref:`scoring_parameter`) or a callable
        (see :ref:`scoring`) to evaluate the predictions on the test set.

        For evaluating multiple metrics, either give a list of (unique)
        strings or a dict with names as keys and callables as values.

        NOTE that when using custom scorers, each scorer should return a
        single value. Metric functions returning a list/array of values
        can be wrapped into multiple scorers that return one value each.

        See :ref:`multimetric_grid_search` for an example.

        .. warning::

           If None, the estimator's default scorer (if available) is used.
           Most scikit-learn estimators will convert large Dask arrays to
           a single NumPy array, which may exhaust the memory of your worker.
           You probably want to always specify `scoring`.

    predict_meta: pd.Series, pd.DataFrame, np.array deafult: None(infer)
        An empty ``pd.Series``, ``pd.DataFrame``, ``np.array`` that matches the output
        type of the estimators ``predict`` call.
        This meta is necessary for  for some estimators to work with
        ``dask.dataframe`` and ``dask.array``

    predict_proba_meta: pd.Series, pd.DataFrame, np.array deafult: None(infer)
        An empty ``pd.Series``, ``pd.DataFrame``, ``np.array`` that matches the output
        type of the estimators ``predict_proba`` call.
        This meta is necessary for  for some estimators to work with
        ``dask.dataframe`` and ``dask.array``

    transform_meta: pd.Series, pd.DataFrame, np.array deafult: None(infer)
        An empty ``pd.Series``, ``pd.DataFrame``, ``np.array`` that matches the output
        type of the estimators ``transform`` call.
        This meta is necessary for  for some estimators to work with
        ``dask.dataframe`` and ``dask.array``

    """

    class_name = "ParallelPostFit"

    def __init__(
        self,
        estimator=None,
        scoring=None,
        predict_meta=None,
        predict_proba_meta=None,
        transform_meta=None,
    ):
        self.estimator = estimator
        self.scoring = scoring
        self.predict_meta = predict_meta
        self.predict_proba_meta = predict_proba_meta
        self.transform_meta = transform_meta

    def _check_array(self, X):
        """Validate an array for post-fit tasks.

        Parameters
        ----------
        X : Union[Array, DataFrame]

        Returns
        -------
        same type as 'X'

        Notes
        -----
        The following checks are applied.

        - Ensure that the array is blocked only along the samples.
        """
        if isinstance(X, da.Array):
            if X.ndim == 2 and X.numblocks[1] > 1:
                logger.debug("auto-rechunking 'X'")
                if not np.isnan(X.chunks[0]).any():
                    X = X.rechunk({0: "auto", 1: -1})
                else:
                    X = X.rechunk({1: -1})
        return X

    @property
    def _postfit_estimator(self):
        # The estimator instance to use for postfit tasks like score
        return self.estimator

    def fit(self, X, y=None, **kwargs):
        """Fit the underlying estimator.

        Parameters
        ----------
        X, y : array-like
        **kwargs
            Additional fit-kwargs for the underlying estimator.

        Returns
        -------
        self : object
        """
        logger.info("Starting fit")
        with _timer("fit", _logger=logger):
            result = self.estimator.fit(X, y, **kwargs)

        # Copy over learned attributes
        copy_learned_attributes(result, self)
        copy_learned_attributes(result, self.estimator)
        return self

    def partial_fit(self, X, y=None, **kwargs):
        logger.info("Starting partial_fit")
        with _timer("fit", _logger=logger):
            result = self.estimator.partial_fit(X, y, **kwargs)

        # Copy over learned attributes
        copy_learned_attributes(result, self)
        copy_learned_attributes(result, self.estimator)
        return self

    def transform(self, X):
        """Transform block or partition-wise for dask inputs.

        For dask inputs, a dask array or dataframe is returned. For other
        inputs (NumPy array, pandas dataframe, scipy sparse matrix), the
        regular return value is returned.

        If the underlying estimator does not have a ``transform`` method, then
        an ``AttributeError`` is raised.

        Parameters
        ----------
        X : array-like

        Returns
        -------
        transformed : array-like
        """
        self._check_method("transform")
        X = self._check_array(X)
        output_meta = self.transform_meta

        if isinstance(X, da.Array):
            if output_meta is None:
                output_meta = _get_output_dask_ar_meta_for_estimator(
                    _transform, self._postfit_estimator, X
                )
            return X.map_blocks(
                _transform, estimator=self._postfit_estimator, meta=output_meta
            )
        elif isinstance(X, dd._Frame):
            if output_meta is None:
                # dask-dataframe relies on dd.core.no_default
                # for infering meta
                output_meta = _transform(X._meta_nonempty, self._postfit_estimator)
            try:
                return X.map_partitions(
                    _transform,
                    self._postfit_estimator,
                    output_meta,
                    meta=output_meta,
                )
            except ValueError:
                return _transform(X, estimator=self._postfit_estimator)
        else:
            return _transform(X, estimator=self._postfit_estimator)

    def score(self, X, y, compute=True):
        """Returns the score on the given data.

        Parameters
        ----------
        X : array-like, shape = [n_samples, n_features]
            Input data, where n_samples is the number of samples and
            n_features is the number of features.

        y : array-like, shape = [n_samples] or [n_samples, n_output], optional
            Target relative to X for classification or regression;
            None for unsupervised learning.

        Returns
        -------
        score : float
                return self.estimator.score(X, y)
        """
        scoring = self.scoring
        X = self._check_array(X)
        y = self._check_array(y)

        if not scoring:
            if type(self._postfit_estimator).score == sklearn.base.RegressorMixin.score:
                scoring = "r2"
            elif (
                type(self._postfit_estimator).score
                == sklearn.base.ClassifierMixin.score
            ):
                scoring = "accuracy"
        else:
            scoring = self.scoring

        if scoring:
            if not dask.is_dask_collection(X) and not dask.is_dask_collection(y):
                scorer = sklearn.metrics.get_scorer(scoring)
            else:
                # TODO: implement Dask-ML's get_scorer() function
                # scorer = get_scorer(scoring, compute=compute)
                pass
            return scorer(self, X, y)
        else:
            return self._postfit_estimator.score(X, y)

    def predict(self, X):
        """Predict for X.

        For dask inputs, a dask array or dataframe is returned. For other
        inputs (NumPy array, pandas dataframe, scipy sparse matrix), the
        regular return value is returned.

        Parameters
        ----------
        X : array-like

        Returns
        -------
        y : array-like
        """
        self._check_method("predict")
        X = self._check_array(X)
        output_meta = self.predict_meta

        if isinstance(X, da.Array):
            if output_meta is None:
                output_meta = _get_output_dask_ar_meta_for_estimator(
                    _predict, self._postfit_estimator, X
                )

            result = X.map_blocks(
                _predict,
                estimator=self._postfit_estimator,
                drop_axis=1,
                meta=output_meta,
            )
            return result

        elif isinstance(X, dd._Frame):
            if output_meta is None:
                # dask-dataframe relies on dd.core.no_default
                # for infering meta
                output_meta = _predict(X._meta_nonempty, self._postfit_estimator)
            try:
                return X.map_partitions(
                    _predict,
                    self._postfit_estimator,
                    output_meta,
                    meta=output_meta,
                )
            except ValueError:
                return _predict(X, estimator=self._postfit_estimator)
        else:
            return _predict(X, estimator=self._postfit_estimator)

    def predict_proba(self, X):
        """Probability estimates.

        For dask inputs, a dask array or dataframe is returned. For other
        inputs (NumPy array, pandas dataframe, scipy sparse matrix), the
        regular return value is returned.

        If the underlying estimator does not have a ``predict_proba``
        method, then an ``AttributeError`` is raised.

        Parameters
        ----------
        X : array or dataframe

        Returns
        -------
        y : array-like
        """
        X = self._check_array(X)

        self._check_method("predict_proba")

        output_meta = self.predict_proba_meta

        if isinstance(X, da.Array):
            if output_meta is None:
                output_meta = _get_output_dask_ar_meta_for_estimator(
                    _predict_proba, self._postfit_estimator, X
                )
            # XXX: multiclass
            return X.map_blocks(
                _predict_proba,
                estimator=self._postfit_estimator,
                meta=output_meta,
                chunks=(X.chunks[0], len(self._postfit_estimator.classes_)),
            )
        elif isinstance(X, dd._Frame):
            if output_meta is None:
                # dask-dataframe relies on dd.core.no_default
                # for infering meta
                output_meta = _predict_proba(X._meta_nonempty, self._postfit_estimator)
            try:
                return X.map_partitions(
                    _predict_proba,
                    self._postfit_estimator,
                    output_meta,
                    meta=output_meta,
                )
            except ValueError:
                return _predict_proba(X, estimator=self._postfit_estimator)
        else:
            return _predict_proba(X, estimator=self._postfit_estimator)

    def predict_log_proba(self, X):
        """Log of probability estimates.

        For dask inputs, a dask array or dataframe is returned. For other
        inputs (NumPy array, pandas dataframe, scipy sparse matrix), the
        regular return value is returned.

        If the underlying estimator does not have a ``predict_proba``
        method, then an ``AttributeError`` is raised.

        Parameters
        ----------
        X : array or dataframe

        Returns
        -------
        y : array-like
        """
        self._check_method("predict_log_proba")
        return da.log(self.predict_proba(X))

    def _check_method(self, method):
        """Check if self.estimator has 'method'.

        Raises
        ------
        AttributeError
        """
        estimator = self._postfit_estimator
        if not hasattr(estimator, method):
            msg = "The wrapped estimator '{}' does not have a '{}' method.".format(
                estimator, method
            )
            raise AttributeError(msg)
        return getattr(estimator, method)


def _first_block(dask_object):
    """Extract the first block / partition from a dask object"""
    if isinstance(dask_object, da.Array):
        if dask_object.ndim > 1 and dask_object.numblocks[-1] != 1:
            raise NotImplementedError(
                "IID estimators require that the array "
                "blocked only along the first axis. "
                "Rechunk your array before fitting."
            )
        shape = (dask_object.chunks[0][0],)
        if dask_object.ndim > 1:
            shape = shape + (dask_object.chunks[1][0],)

        return da.from_delayed(
            dask_object.to_delayed().flatten()[0], shape, dask_object.dtype
        )

    if isinstance(dask_object, dd._Frame):
        return dask_object.get_partition(0)

    else:
        return dask_object


def _predict(part, estimator, output_meta=None):
    if part.shape[0] == 0 and output_meta is not None:
        empty_output = handle_empty_partitions(output_meta)
        if empty_output is not None:
            return empty_output
    return estimator.predict(part)


def _predict_proba(part, estimator, output_meta=None):
    if part.shape[0] == 0 and output_meta is not None:
        empty_output = handle_empty_partitions(output_meta)
        if empty_output is not None:
            return empty_output
    return estimator.predict_proba(part)


def _transform(part, estimator, output_meta=None):
    if part.shape[0] == 0 and output_meta is not None:
        empty_output = handle_empty_partitions(output_meta)
        if empty_output is not None:
            return empty_output
    return estimator.transform(part)


def handle_empty_partitions(output_meta):
    if hasattr(output_meta, "__array_function__"):
        if len(output_meta.shape) == 1:
            shape = 0
        else:
            shape = list(output_meta.shape)
            shape[0] = 0
        ar = np.zeros(
            shape=shape,
            dtype=output_meta.dtype,
            like=output_meta,
        )
        return ar
    elif "scipy.sparse" in type(output_meta).__module__:
        # sparse matrices don't support
        # `like` due to non implemented __array_function__
        # Refer https://github.com/scipy/scipy/issues/10362
        # Note below works for both cupy and scipy sparse matrices
        if len(output_meta.shape) == 1:
            shape = 0
        else:
            shape = list(output_meta.shape)
            shape[0] = 0
        ar = type(output_meta)(shape, dtype=output_meta.dtype)
        return ar
    elif hasattr(output_meta, "iloc"):
        return output_meta.iloc[:0, :]


def _get_output_dask_ar_meta_for_estimator(model_fn, estimator, input_dask_ar):
    """
    Returns the output metadata array
    for the model function (predict, transform etc)
    by running the appropriate function on dummy data
    of shape (1, n_features)

    Parameters
    ----------

    model_fun: Model function
        _predict, _transform etc

    estimator : Estimator
        The underlying estimator that is fit.

    input_dask_ar: The input dask_array

    Returns
    -------
    metadata: metadata of  output dask array

    """
    # sklearn fails if input array has size size
    # It requires at least 1 sample to run successfully
    input_meta = input_dask_ar._meta
    if hasattr(input_meta, "__array_function__"):
        ar = np.zeros(
            shape=(1, input_dask_ar.shape[1]),
            dtype=input_dask_ar.dtype,
            like=input_meta,
        )
    elif "scipy.sparse" in type(input_meta).__module__:
        # sparse matrices dont support
        # `like` due to non implimented __array_function__
        # Refer https://github.com/scipy/scipy/issues/10362
        # Note below works for both cupy and scipy sparse matrices
        ar = type(input_meta)((1, input_dask_ar.shape[1]), dtype=input_dask_ar.dtype)
    else:
        func_name = model_fn.__name__.strip("_")
        msg = (
            f"Metadata for {func_name} is not provided, so Dask is "
            f"running the {func_name} "
            "function on a small dataset to guess output metadata. "
            "As a result, It is possible that Dask will guess incorrectly."
        )
        warnings.warn(msg)
        ar = np.zeros(shape=(1, input_dask_ar.shape[1]), dtype=input_dask_ar.dtype)
    return model_fn(ar, estimator)


def copy_learned_attributes(from_estimator, to_estimator):
    attrs = {k: v for k, v in vars(from_estimator).items() if k.endswith("_")}

    for k, v in attrs.items():
        setattr(to_estimator, k, v)


@contextlib.contextmanager
def _timer(name, _logger=None, level="info"):
    """
    Output execution time of a function to the given logger level
    Parameters
    ----------
    name : str
        How to name the timer (will be in the logs)
    logger : logging.logger
        The optional logger where to write
    level : str
        On which level to log the performance measurement
    """
    start = tic()
    _logger = _logger or logger
    _logger.info("Starting %s", name)
    yield
    stop = tic()
    delta = datetime.timedelta(seconds=stop - start)
    _logger_level = getattr(_logger, level)
    _logger_level("Finished %s in %s", name, delta)  # nicer formatting for time.
