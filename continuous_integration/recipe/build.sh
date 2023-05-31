#!/bin/bash

export RUST_BACKTRACE=full

# pull from existing rust cache, if it exists
if [ -d "~/.cargo/registry" ]; then ln -s ~/.cargo/registry $CONDA_PREFIX/.cargo/registry; fi
if [ -d "~/.cargo/git" ]; then ln -s ~/.cargo/git $CONDA_PREFIX/.cargo/git; fi

$PYTHON -m pip install . --no-deps -vv
