# Copyright 2017, Dask developers
# Dask-ML project - https://github.com/dask/dask-ml
from collections.abc import Sequence

import dask
import dask.array as da
import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest
from dask.array.utils import assert_eq as assert_eq_ar
from dask.dataframe.utils import assert_eq as assert_eq_df
from sklearn.base import clone
from sklearn.decomposition import PCA
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression, SGDClassifier

from dask_sql.physical.rel.custom.wrappers import Incremental, ParallelPostFit


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_ml_class_mappings(gpu):
    from dask_sql.physical.utils.ml_classes import get_cpu_classes, get_gpu_classes
    from dask_sql.utils import import_class

    try:
        import lightgbm
        import xgboost
    except KeyError:
        lightgbm = None
        xgboost = None

    classes_dict = get_gpu_classes() if gpu else get_cpu_classes()

    for key in classes_dict:
        if not ("XGB" in key and xgboost is None) and not (
            "LGBM" in key and lightgbm is None
        ):
            import_class(classes_dict[key])


def _check_axis_partitioning(chunks, n_features):
    c = chunks[1][0]
    if c != n_features:
        msg = (
            "Can only generate arrays partitioned along the "
            "first axis. Specifying a larger chunksize for "
            "the second axis.\n\n\tchunk size: {}\n"
            "\tn_features: {}".format(c, n_features)
        )
        raise ValueError(msg)


def check_random_state(random_state):
    if random_state is None:
        return da.random.RandomState()
    # elif isinstance(random_state, Integral):
    #     return da.random.RandomState(random_state)
    elif isinstance(random_state, np.random.RandomState):
        return da.random.RandomState(random_state.randint())
    elif isinstance(random_state, da.random.RandomState):
        return random_state
    else:
        raise TypeError(f"Unexpected type '{type(random_state)}'")


def make_classification(
    n_samples=100,
    n_features=20,
    n_informative=2,
    n_classes=2,
    scale=1.0,
    random_state=None,
    chunks=None,
):
    chunks = da.core.normalize_chunks(chunks, (n_samples, n_features))
    _check_axis_partitioning(chunks, n_features)

    if n_classes != 2:
        raise NotImplementedError("n_classes != 2 is not yet supported.")

    rng = check_random_state(random_state)

    X = rng.normal(0, 1, size=(n_samples, n_features), chunks=chunks)
    informative_idx = rng.choice(n_features, n_informative, chunks=n_informative)
    beta = (rng.random(n_features, chunks=n_features) - 1) * scale

    informative_idx, beta = dask.compute(
        informative_idx, beta, scheduler="single-threaded"
    )

    z0 = X[:, informative_idx].dot(beta[informative_idx])
    y = rng.random(z0.shape, chunks=chunks[0]) < 1 / (1 + da.exp(-z0))
    y = y.astype(int)

    return X, y


def _assert_eq(l, r, name=None, **kwargs):
    array_types = (np.ndarray, da.Array)
    frame_types = (pd.core.generic.NDFrame, dd._Frame)
    if isinstance(l, array_types):
        assert_eq_ar(l, r, **kwargs)
    elif isinstance(l, frame_types):
        assert_eq_df(l, r, **kwargs)
    elif isinstance(l, Sequence) and any(
        isinstance(x, array_types + frame_types) for x in l
    ):
        for a, b in zip(l, r):
            _assert_eq(a, b, **kwargs)
    elif np.isscalar(r) and np.isnan(r):
        assert np.isnan(l), (name, l, r)
    else:
        assert l == r, (name, l, r)


def assert_estimator_equal(left, right, exclude=None, **kwargs):
    """Check that two Estimators are equal
    Parameters
    ----------
    left, right : Estimators
    exclude : str or sequence of str
        attributes to skip in the check
    kwargs : dict
        Passed through to the dask `assert_eq` method.
    """
    left_attrs = [x for x in dir(left) if x.endswith("_") and not x.startswith("_")]
    right_attrs = [x for x in dir(right) if x.endswith("_") and not x.startswith("_")]
    if exclude is None:
        exclude = set()
    elif isinstance(exclude, str):
        exclude = {exclude}
    else:
        exclude = set(exclude)

    left_attrs2 = set(left_attrs) - exclude
    right_attrs2 = set(right_attrs) - exclude

    assert left_attrs2 == right_attrs2, left_attrs2 ^ right_attrs2

    for attr in left_attrs2:
        l = getattr(left, attr)
        r = getattr(right, attr)
        _assert_eq(l, r, name=attr, **kwargs)


def test_parallelpostfit_basic():
    clf = ParallelPostFit(GradientBoostingClassifier())

    X, y = make_classification(n_samples=1000, chunks=100)
    X_, y_ = dask.compute(X, y)
    clf.fit(X_, y_)

    assert isinstance(clf.predict(X), da.Array)
    assert isinstance(clf.predict_proba(X), da.Array)

    result = clf.score(X, y)
    expected = clf.estimator.score(X_, y_)
    assert result == expected


@pytest.mark.parametrize("kind", ["numpy", "dask.dataframe", "dask.array"])
def test_predict(kind):
    X, y = make_classification(chunks=100)

    if kind == "numpy":
        X, y = dask.compute(X, y)
    elif kind == "dask.dataframe":
        X = dd.from_dask_array(X)
        y = dd.from_dask_array(y)

    base = LogisticRegression(random_state=0, n_jobs=1, solver="lbfgs")
    wrap = ParallelPostFit(
        LogisticRegression(random_state=0, n_jobs=1, solver="lbfgs"),
    )

    base.fit(*dask.compute(X, y))
    wrap.fit(*dask.compute(X, y))

    assert_estimator_equal(wrap.estimator, base)

    result = wrap.predict(X)
    expected = base.predict(X)
    assert_eq_ar(result, expected)

    result = wrap.predict_proba(X)
    expected = base.predict_proba(X)
    assert_eq_ar(result, expected)

    result = wrap.predict_log_proba(X)
    expected = base.predict_log_proba(X)
    assert_eq_ar(result, expected)


@pytest.mark.parametrize("kind", ["numpy", "dask.dataframe", "dask.array"])
def test_transform(kind):
    X, y = make_classification(chunks=100)

    if kind == "numpy":
        X, y = dask.compute(X, y)
    elif kind == "dask.dataframe":
        X = dd.from_dask_array(X)
        y = dd.from_dask_array(y)

    base = PCA(random_state=0)
    wrap = ParallelPostFit(PCA(random_state=0))

    base.fit(*dask.compute(X, y))
    wrap.fit(*dask.compute(X, y))

    assert_estimator_equal(wrap.estimator, base)

    result = base.transform(*dask.compute(X))
    expected = wrap.transform(X)
    assert_eq_ar(result, expected)


@pytest.mark.parametrize("dataframes", [False, True])
def test_incremental_basic(dataframes):
    # Create observations that we know linear models can recover
    n, d = 100, 3
    rng = da.random.RandomState(42)
    X = rng.normal(size=(n, d), chunks=30)
    coef_star = rng.uniform(size=d, chunks=d)
    y = da.sign(X.dot(coef_star))
    y = (y + 1) / 2
    if dataframes:
        X = dd.from_array(X)
        y = dd.from_array(y)

    est1 = SGDClassifier(random_state=0, tol=1e-3, average=True)
    est2 = clone(est1)

    clf = Incremental(est1, random_state=0)
    result = clf.fit(X, y, classes=[0, 1])
    assert result is clf

    # est2 is a sklearn optimizer; this is just a benchmark
    if dataframes:
        X = X.to_dask_array(lengths=True)
        y = y.to_dask_array(lengths=True)

    for slice_ in da.core.slices_from_chunks(X.chunks):
        est2.partial_fit(X[slice_].compute(), y[slice_[0]].compute(), classes=[0, 1])

    assert isinstance(result.estimator_.coef_, np.ndarray)
    rel_error = np.linalg.norm(clf.coef_ - est2.coef_)
    rel_error /= np.linalg.norm(clf.coef_)
    assert rel_error < 0.9

    assert set(dir(clf.estimator_)) == set(dir(est2))

    #  Predict
    result = clf.predict(X)
    expected = est2.predict(X)
    assert isinstance(result, da.Array)
    if dataframes:
        # Compute is needed because chunk sizes of this array are unknown
        result = result.compute()
    rel_error = np.linalg.norm(result - expected)
    rel_error /= np.linalg.norm(expected)
    assert rel_error < 0.3

    # score
    result = clf.score(X, y)
    expected = est2.score(*dask.compute(X, y))
    assert abs(result - expected) < 0.1

    clf = Incremental(SGDClassifier(random_state=0, tol=1e-3, average=True))
    clf.partial_fit(X, y, classes=[0, 1])
    assert set(dir(clf.estimator_)) == set(dir(est2))
