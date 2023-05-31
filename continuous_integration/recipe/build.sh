#!/bin/bash

export RUST_BACKTRACE=full

# set up caching if we are being executed in a github workflow
if [[ -v GITHUB_WORKSPACE ]]; then
    echo "GITHUB_WORKSPACE is set so assuming we are building from a GHA workflow; searching for cached work directory..."
    if [[ ! -d "$GITHUB_WORKSPACE/dask_planner/target" ]]; then
        echo "No cached work directory found; creating a new one..."
        mkdir "$GITHUB_WORKSPACE/dask_planner/target"
    fi
    echo "Symlinking work directory to conda build work directory..."
    ln -s $GITHUB_WORKSPACE/dask_planner/target $CONDA_PREFIX/../work/dask_planner/target
fi

# pull cached registry from existing rust installation, if any
if [[ -d "$HOME/.cargo/registry" ]]; then
    echo "Symlinking global Rust registry to conda build environment..."
    ln -s $HOME/.cargo/registry $CONDA_PREFIX/.cargo/registry
fi
if [[ -d "$HOME/.cargo/git" ]]; then
    echo "Symlinking global Rust git checkouts to conda build environment..."
    ln -s $HOME/.cargo/git $CONDA_PREFIX/.cargo/git
fi

$PYTHON -m pip install . --no-deps -vv
