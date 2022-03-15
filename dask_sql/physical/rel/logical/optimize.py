import operator

import dask.dataframe as dd
import numpy as np
from dask.blockwise import Blockwise
from dask.layers import DataFrameIOLayer
from dask.utils import M, apply, is_arraylike

_comparison_ops_and_exprs = {
    operator.eq: ({"func": operator.eq}, "=="),
    operator.ne: ({"func": operator.ne}, "!="),
    operator.lt: ({"func": operator.lt}, "<"),
    operator.le: ({"func": operator.le}, "<="),
    operator.gt: ({"func": operator.gt}, ">"),
    operator.ge: ({"func": operator.ge}, ">="),
    np.greater: ({"func": np.greater}, ">"),
    np.greater_equal: ({"func": np.greater_equal}, ">="),
    np.less: ({"func": np.less}, "<"),
    np.less_equal: ({"func": np.less_equal}, "<="),
    np.equal: ({"func": np.equal}, "=="),
    np.not_equal: ({"func": np.not_equal}, "!="),
}
_comparison_ops = {k: v[0] for k, v in _comparison_ops_and_exprs.items()}
_comparison_symbols = {k: v[1] for k, v in _comparison_ops_and_exprs.items()}
_supported_ops = {
    **_comparison_ops.copy(),
    operator.and_: {"func": operator.and_},
    operator.or_: {"func": operator.or_},
    operator.getitem: {"func": operator.getitem},
    M.fillna: {"func": dd.Series.fillna},
}


class SimpleDispatch:

    """Simple dispatch class"""

    def __init__(self, name=None):
        self._lookup = {}
        if name:
            self.__name__ = name

    def register(self, ref, func=None):
        """Register dispatch of `func` on `ref`"""

        def wrapper(func):
            if isinstance(ref, tuple):
                for t in ref:
                    self.register(t, func)
            else:
                self._lookup[ref] = func
            return func

        return wrapper(func) if func is not None else wrapper

    def dispatch(self, ref):
        """Return the function implementation for the given ``ref``"""
        lk = self._lookup
        try:
            return lk[ref]
        except KeyError:
            pass
        raise TypeError(f"No dispatch for {ref}")

    def __call__(self, arg, *args, **kwargs):
        """
        Call the corresponding method based on type of argument.
        """
        meth = self.dispatch(arg)
        return meth(arg, *args, **kwargs)


dnf_filter_dispatch = SimpleDispatch("dnf_filter_dispatch")


def _get_blockwise_input(input_index, indices, dsk):
    key = indices[input_index][0]
    if indices[input_index][1] is None:
        return key
    return dsk.layers[key]._dnf_filter_expression(dsk)


_inv_symbol = {
    ">": "<",
    "<": ">",
    ">=": "<=",
    "<=": ">=",
}


def _inv(symbol):
    return _inv_symbol.get(symbol, symbol)


@dnf_filter_dispatch.register(tuple(_comparison_symbols.keys()))
def comparison_dnf(op, indices: list, dsk):
    left = _get_blockwise_input(0, indices, dsk)
    right = _get_blockwise_input(1, indices, dsk)
    if is_arraylike(left) and hasattr(left, "item") and left.size == 1:
        left = left.item()
        return (right, _inv(_comparison_symbols[op]), left)
    if is_arraylike(right) and hasattr(right, "item") and right.size == 1:
        right = right.item()
    return (left, _comparison_symbols[op], right)


def _maybe_list(val):
    if isinstance(val, tuple) and val and isinstance(val[0], (tuple, list)):
        return list(val)
    return [val]


@dnf_filter_dispatch.register((operator.and_, operator.or_))
def logical_dnf(op, indices: list, dsk):
    left = _get_blockwise_input(0, indices, dsk)
    right = _get_blockwise_input(1, indices, dsk)
    if op == operator.or_:
        return _maybe_list(left), _maybe_list(right)
    elif op == operator.and_:
        return (left, right)
    else:
        raise ValueError


@dnf_filter_dispatch.register(operator.getitem)
def getitem_dnf(op, indices: list, dsk):
    # Return dnf of key (selected by getitem)
    key = _get_blockwise_input(1, indices, dsk)
    return key


@dnf_filter_dispatch.register(dd.Series.fillna)
def fillna_dnf(op, indices: list, dsk):
    # Return dnf of input collection
    return _get_blockwise_input(0, indices, dsk)


class RegenerableLayer:
    def __init__(self, layer, creation_info):
        self.layer = layer
        self.creation_info = creation_info

    def _regenerate_collection(
        self, dsk, new_kwargs: dict = None, _regen_cache: dict = None,
    ):

        # Return regenerated layer if the work was
        # already done
        _regen_cache = _regen_cache or {}
        if self.layer.output in _regen_cache:
            return _regen_cache[self.layer.output]

        # Recursively generate necessary inputs to
        # this layer to generate the collection
        inputs = []
        for key, ind in self.layer.indices:
            if ind is None:
                if isinstance(key, (str, tuple)) and key in dsk.layers:
                    continue
                inputs.append(key)
            elif key in self.layer.io_deps:
                continue
            else:
                inputs.append(
                    dsk.layers[key]._regenerate_collection(
                        dsk, new_kwargs=new_kwargs, _regen_cache=_regen_cache,
                    )
                )

        # Extract the callable func and key-word args.
        # Then return a regenerated collection
        func = self.creation_info.get("func", None)
        if func is None:
            raise ValueError(
                "`_regenerate_collection` failed. "
                "Not all HLG layers are regenerable."
            )
        regen_args = self.creation_info.get("args", [])
        regen_kwargs = self.creation_info.get("kwargs", {}).copy()
        regen_kwargs = {k: v for k, v in self.creation_info.get("kwargs", {}).items()}
        regen_kwargs.update((new_kwargs or {}).get(self.layer.output, {}))
        result = func(*inputs, *regen_args, **regen_kwargs)
        _regen_cache[self.layer.output] = result
        return result

    def _dnf_filter_expression(self, dsk):
        """Return a DNF-formatted filter expression for the
        graph terminating at this layer
        """
        return dnf_filter_dispatch(self.creation_info["func"], self.layer.indices, dsk,)


class RegenerableGraph:
    def __init__(self, layers, dependencies, dependents):
        self.layers = layers
        self.dependencies = dependencies
        self.dependents = dependents

    @classmethod
    def from_hlg(cls, graph):
        _layers = {}
        for key, layer in graph.layers.items():
            regenerable_layer = None
            if isinstance(layer, DataFrameIOLayer):
                regenerable_layer = RegenerableLayer(layer, layer.creation_info or {},)
            elif isinstance(layer, Blockwise):
                tasks = list(layer.dsk.values())
                if len(tasks) == 1 and tasks[0]:
                    if tasks[0][0] == apply:
                        creation_info = _supported_ops.get(tasks[0][1], None)
                    else:
                        creation_info = _supported_ops.get(tasks[0][0], None)
                    if creation_info:
                        regenerable_layer = RegenerableLayer(layer, creation_info)

            if regenerable_layer is None:
                raise ValueError(f"Graph contains non-regenerable layer: {layer}")

            _layers[key] = regenerable_layer

        return RegenerableGraph(
            _layers, graph.dependencies.copy(), graph.dependents.copy(),
        )


def predicate_pushdown(ddf):

    # Get output layer name and HLG
    name = ddf._name

    # Start by converting the HLG to RegenerableGraph
    try:
        dsk = RegenerableGraph.from_hlg(ddf.dask)
    except ValueError:
        return ddf

    # Extract filters
    try:
        filters = dsk.layers[name]._dnf_filter_expression(dsk)
        if filters:
            if isinstance(filters[0], (list, tuple)):
                filters = list(filters)
            else:
                filters = [filters]
        else:
            return ddf
        if not isinstance(filters, list):
            filters = [filters]
    except ImportError:  # (TypeError, ValueError):
        # DNF dispatching failed for 1+ layers
        return ddf

    # We were able to extract a DNF filter expression.
    # Check that all layers are regenerable, and that
    # the graph contains an IO layer with filters support.
    # All layers besides the root IO layer should also
    # support DNF dispatching.  Otherwise, there could be
    # something like column-assignment or data manipulation
    # between the IO layer and the filter.
    io_layer = []
    for k, v in dsk.layers.items():
        if (
            isinstance(v.layer, DataFrameIOLayer)
            and "filters" in v.creation_info.get("kwargs", {})
            and v.creation_info["kwargs"]["filters"] is None
        ):
            io_layer.append(k)
    if len(io_layer) != 1:
        return ddf
    io_layer = io_layer.pop()

    # Regenerate collection with filtered IO layer
    return dsk.layers[name]._regenerate_collection(
        dsk, new_kwargs={io_layer: {"filters": filters}},
    )
