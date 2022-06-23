# Contributing to Dask-SQL

## SQL Parsing Overview

![Dask-SQL Sequence Diagram](assets/dask-sql-sequence.jpg)

## Environment Setup

### Conda
Conda is used both by CI and the development team. Therefore Conda is the fully supported and preferred method for using and developing Dask-SQL.

Installing Conda is outside the scope of this document. However a nice guide for installing on Linux can be found [here](https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html)

Setting up your Conda environment for development is straightforward. To setup a new Conda environment you run
```
conda env create -f {DASK_SQL_HOME}/continuous_integration/environment-3.10-dev.yaml
```

The Conda process will take awhile to complete, once finished you will have a resulting environment named `dask-sql` which can be activated and used by running `conda activate dask-sql`

## Python Developers Guide
TODO

## Rust Developers Guide

Dask-SQL utilizes [Apache Arrow Datafusion](https://github.com/apache/arrow-datafusion) for parsing, planning, and optimizing SQL queries. DataFusion is written in Rust and therefore requires some Rust experience to be productive. Luckily, there are tons of great Rust learning resources on the internet. We have listed some of our favorite ones [here](#rust-learning-resources)

### Building
Building the Dask-SQL Rust codebase is a straightforward process. If you create and activate the Dask-SQL Conda environment the Rust compiler and all necessary components will be installed for you during that process and therefore requires no further manual setup.

`setuptools-rust` is used by Dask-SQL for building and bundling the resulting Rust binaries. This helps make building and installing the Rust binaries feel much more like a native Python workflow.

More details about the building setup can be found at [setup.py](setup.py) and searching for `rust_extensions` which is the hook for the Rust code build and inclusion.

Note that while `setuptools-rust` is used by CI and should be used during your development cycle, if the need arises to do something more specific that is not yet supported by `setuptools-rust` you can opt to use `cargo` directly from the command line.

#### Cleaning
```python setup.py clean```
#### Building
```python setup.py install```

### Rust Learning Resources
- ["The Book"](https://doc.rust-lang.org/book/)
- [Lets Get Rusty "LGR" YouTube series](https://www.youtube.com/c/LetsGetRusty)

## Documentation TODO
- [ ] Python developers guide section
- [ ] SQL Parsing overview diagram
- [ ] Architecture diagram
- [x] Setup dev environment
- [x] Version of Rust and specs
- [ ] Updating version of datafusion
- [x] building
- [ ] testing
- [x
] Rust learning resources
- [ ] Types mapping, Arrow datatypes
- [ ] RexTypes explaination, show simple query and show it broken down into its parts in a diagram
- [ ] Understand Rust code layout
- [ ] Rust Datastructures local to Dask-SQL
- [ ] links and notes on how to build DataFusion documentation locally
- [ ] Registering tables with DaskSqlContext, also functions
- [ ] PyO3 documentation links
- [ ] Short section showing how Rust works with PyO3

## Rust Learning Resources
 - TODO, need to build this list ...
