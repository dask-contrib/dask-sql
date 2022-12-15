# Copyright 2017, Dask developers
# Dask-ML project - https://github.com/dask/dask-ml
from typing import Optional, TypeVar

import dask
import dask.array as da
import numpy as np
import sklearn.metrics
import sklearn.utils.multiclass
from dask.array import Array
from dask.utils import derived_from

ArrayLike = TypeVar("ArrayLike", Array, np.ndarray)


def accuracy_score(
    y_true: ArrayLike,
    y_pred: ArrayLike,
    normalize: bool = True,
    sample_weight: Optional[ArrayLike] = None,
    compute: bool = True,
) -> ArrayLike:
    """Accuracy classification score.
    In multilabel classification, this function computes subset accuracy:
    the set of labels predicted for a sample must *exactly* match the
    corresponding set of labels in y_true.
    Read more in the :ref:`User Guide <accuracy_score>`.
    Parameters
    ----------
    y_true : 1d array-like, or label indicator array
        Ground truth (correct) labels.
    y_pred : 1d array-like, or label indicator array
        Predicted labels, as returned by a classifier.
    normalize : bool, optional (default=True)
        If ``False``, return the number of correctly classified samples.
        Otherwise, return the fraction of correctly classified samples.
    sample_weight : 1d array-like, optional
        Sample weights.
        .. versionadded:: 0.7.0
    Returns
    -------
    score : scalar dask Array
        If ``normalize == True``, return the correctly classified samples
        (float), else it returns the number of correctly classified samples
        (int).
        The best performance is 1 with ``normalize == True`` and the number
        of samples with ``normalize == False``.
    Notes
    -----
    In binary and multiclass classification, this function is equal
    to the ``jaccard_similarity_score`` function.

    """

    if y_true.ndim > 1:
        differing_labels = ((y_true - y_pred) == 0).all(1)
        score = differing_labels != 0
    else:
        score = y_true == y_pred

    if normalize:
        score = da.average(score, weights=sample_weight)
    elif sample_weight is not None:
        score = da.dot(score, sample_weight)
    else:
        score = score.sum()

    if compute:
        score = score.compute()
    return score


def _log_loss_inner(
    x: ArrayLike, y: ArrayLike, sample_weight: Optional[ArrayLike], **kwargs
):
    # da.map_blocks wasn't able to concatenate together the results
    # when we reduce down to a scalar per block. So we make an
    # array with 1 element.
    if sample_weight is not None:
        sample_weight = sample_weight.ravel()
    return np.array(
        [sklearn.metrics.log_loss(x, y, sample_weight=sample_weight, **kwargs)]
    )


def log_loss(
    y_true, y_pred, eps=1e-15, normalize=True, sample_weight=None, labels=None
):
    if not (dask.is_dask_collection(y_true) and dask.is_dask_collection(y_pred)):
        return sklearn.metrics.log_loss(
            y_true,
            y_pred,
            eps=eps,
            normalize=normalize,
            sample_weight=sample_weight,
            labels=labels,
        )

    if y_pred.ndim > 1 and y_true.ndim == 1:
        y_true = y_true.reshape(-1, 1)
        drop_axis: Optional[int] = 1
        if sample_weight is not None:
            sample_weight = sample_weight.reshape(-1, 1)
    else:
        drop_axis = None

    result = da.map_blocks(
        _log_loss_inner,
        y_true,
        y_pred,
        sample_weight,
        chunks=(1,),
        drop_axis=drop_axis,
        dtype="f8",
        eps=eps,
        normalize=normalize,
        labels=labels,
    )
    if normalize and sample_weight is not None:
        sample_weight = sample_weight.ravel()
        block_weights = sample_weight.map_blocks(np.sum, chunks=(1,), keepdims=True)
        return da.average(result, 0, weights=block_weights)
    elif normalize:
        return result.mean()
    else:
        return result.sum()


def _check_sample_weight(sample_weight: Optional[ArrayLike]):
    if sample_weight is not None:
        raise ValueError("'sample_weight' is not supported.")


@derived_from(sklearn.metrics)
def mean_squared_error(
    y_true: ArrayLike,
    y_pred: ArrayLike,
    sample_weight: Optional[ArrayLike] = None,
    multioutput: Optional[str] = "uniform_average",
    squared: bool = True,
    compute: bool = True,
) -> ArrayLike:
    _check_sample_weight(sample_weight)
    output_errors = ((y_pred - y_true) ** 2).mean(axis=0)

    if isinstance(multioutput, str) or multioutput is None:
        if multioutput == "raw_values":
            if compute:
                return output_errors.compute()
            else:
                return output_errors
    else:
        raise ValueError("Weighted 'multioutput' not supported.")
    result = output_errors.mean()
    if not squared:
        result = da.sqrt(result)
    if compute:
        result = result.compute()
    return result


def _check_reg_targets(
    y_true: ArrayLike, y_pred: ArrayLike, multioutput: Optional[str]
):
    if multioutput is not None and multioutput != "uniform_average":
        raise NotImplementedError("'multioutput' must be 'uniform_average'")

    if y_true.ndim == 1:
        y_true = y_true.reshape((-1, 1))
    if y_pred.ndim == 1:
        y_pred = y_pred.reshape((-1, 1))

    # TODO: y_type, multioutput
    return None, y_true, y_pred, multioutput


@derived_from(sklearn.metrics)
def r2_score(
    y_true: ArrayLike,
    y_pred: ArrayLike,
    sample_weight: Optional[ArrayLike] = None,
    multioutput: Optional[str] = "uniform_average",
    compute: bool = True,
) -> ArrayLike:
    _check_sample_weight(sample_weight)
    _, y_true, y_pred, _ = _check_reg_targets(y_true, y_pred, multioutput)
    weight = 1.0

    numerator = (weight * (y_true - y_pred) ** 2).sum(axis=0, dtype="f8")
    denominator = (weight * (y_true - y_true.mean(axis=0)) ** 2).sum(axis=0, dtype="f8")

    nonzero_denominator = denominator != 0
    nonzero_numerator = numerator != 0
    valid_score = nonzero_denominator & nonzero_numerator
    output_chunks = getattr(y_true, "chunks", [None, None])[1]
    output_scores = da.ones([y_true.shape[1]], chunks=output_chunks)
    with np.errstate(all="ignore"):
        output_scores[valid_score] = 1 - (
            numerator[valid_score] / denominator[valid_score]
        )
        output_scores[nonzero_numerator & ~nonzero_denominator] = 0.0

    result = output_scores.mean(axis=0)
    if compute:
        result = result.compute()
    return result
