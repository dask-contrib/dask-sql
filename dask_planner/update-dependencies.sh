#!/bin/bash

UPDATE_ALL_CARGO_DEPS="${UPDATE_ALL_CARGO_DEPS:-true}"
# Update datafusion dependencies in the dask-planner to the latest revision from the default branch
sed -i -r 's/^datafusion-([a-z]+).*/datafusion-\1 = { git = "https:\/\/github.com\/apache\/arrow-datafusion\/" }/g' Cargo.toml

if [ "$UPDATE_ALL_CARGO_DEPS" = true ] ; then
    cargo update
fi
