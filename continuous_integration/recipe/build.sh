#!/bin/bash

export RUST_BACKTRACE=full

# pull from existing rust cache, if it exists
if [ -d "~/.cargo/registry" ]; then
    ln -s ~/.cargo/registry $CONDA_PREFIX/.cargo/registry
else
if [ -d "~/.cargo/git" ]; then
    ln -s ~/.cargo/git $CONDA_PREFIX/.cargo/git
else

$PYTHON -m pip install . --no-deps -vv
