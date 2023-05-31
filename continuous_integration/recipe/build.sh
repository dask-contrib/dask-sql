#!/bin/bash

export RUST_BACKTRACE=full

# pull from existing rust cache, if it exists
if [ -d "$HOME/.cargo/registry" ]; then ln -s $HOME/.cargo/registry $CONDA_PREFIX/.cargo/registry; fi
if [ -d "$HOME/.cargo/git" ]; then ln -s $HOME/.cargo/git $CONDA_PREFIX/.cargo/git; fi

$PYTHON -m pip install . --no-deps -vv
