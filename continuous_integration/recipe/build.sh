#!/bin/bash

export RUST_BACKTRACE=full

# set up caching if we are being executed in a github workflow
if [[ -v GITHUB_WORKSPACE ]]; then
    if [[ ! -d "$GITHUB_WORKSPACE/dask_planner/target" ]]; then
        mkdir "$GITHUB_WORKSPACE/dask_planner/target"
    fi
    ln -s $GITHUB_WORKSPACE/dask_planner/target $CONDA_PREFIX/../work/dask_planner/target
fi

# pull cached registry from existing rust installation, if any
if [ -d "$HOME/.cargo/registry" ]; then ln -s $HOME/.cargo/registry $CONDA_PREFIX/.cargo/registry; fi
if [ -d "$HOME/.cargo/git" ]; then ln -s $HOME/.cargo/git $CONDA_PREFIX/.cargo/git; fi

$PYTHON -m pip install . --no-deps -vv
