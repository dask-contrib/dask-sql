#!/bin/bash

export RUST_BACKTRACE=full

# pull cached work directory from previous compilations, if any
if [[ ! -d "$RECIPE_DIR/../../dask_planner/target" ]]; then
    echo "No cached work directory found; creating a new one..."
    mkdir "$RECIPE_DIR/../../dask_planner/target"
fi
echo "Symlinking work directory to conda build work directory..."
ln -s $RECIPE_DIR/../../dask_planner/target $CONDA_PREFIX/../work/dask_planner/target

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

# remove symlinks before proceeding to make sure that cached files aren't cleaned up by conda build
rm $CONDA_PREFIX/../work/dask_planner/target
rm $CONDA_PREFIX/.cargo/registry
rm $CONDA_PREFIX/.cargo/git
