#!/bin/bash

export RUST_BACKTRACE=full

# pull cached work directory if we are in a github workflow
# if [[ -v GITHUB_WORKSPACE && -d "$GITHUB_WORKSPACE/dask_planner/target" ]]; then
#     ln -s $GITHUB_WORKSPACE/dask_planner/target $CONDA_PREFIX/../work/dask_planner/target
# if

# pull cached registry from existing rust installation, if any
if [ -d "$HOME/.cargo/registry" ]; then ln -s $HOME/.cargo/registry $CONDA_PREFIX/.cargo/registry; fi
if [ -d "$HOME/.cargo/git" ]; then ln -s $HOME/.cargo/git $CONDA_PREFIX/.cargo/git; fi

$PYTHON -m pip install . --no-deps -vv
