# Contributing to Dask-SQL

## Architecture Diagram

![Dask-SQL Sequence Diagram](assets/dask-sql-sequence.jpg)

## Rust Developers Guide

Dask-SQL utilizes [Apache Arrow Datafusion](https://github.com/apache/arrow-datafusion) for parsing, planning, and optimizing SQL query strings. DataFusion is written in Rust and therefore requires some Rust experience to be productive. Luckily, there are tons of great Rust learning resources on the internet. We have listed some of our favorite ones [here](#rust-learning-resources)

### Environment Setup

#### Prerequisites

The bare minimum requirements to get started with development are listed below. The complete list of requirements can be seen at [conda environment file](continuous_integration/environment-3.10-dev.yaml) and is generally the best place to determine the latest version requirements.

##### Conda Based - Recommended
Conda is used both by CI and the development team. Therefore Conda is the fully supported and preferred method for using and developing Dask-SQL.

Installing Conda is outside the scope of this document. However a nice guide for installing on Linux can be found [here](https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html)

Setting up your Conda environment for development is straightforward. To setup a new Conda environment you run
```
conda env create -f {DASK_SQL_HOME}/continuous_integration/environment-3.10-dev.yaml
```

The Conda process will take awhile to complete, once finished you will have a resulting environment named `dask-sql` which can be activated and used by running `conda activate dask-sql`

##### Standalone

- `Python` >= 3.8
- `rust` >= 1.60.0
- `setuptools-rust` >= 1.1.2
    - This handle package will setup and handle building your Rust code for you

## Documentation TODO
- [ ] Architecture Diagram
- [x] Setup dev environment
- [ ] Version of Rust and specs
- [ ] Updating version of datafusion
- [ ] building
- [ ] testing
- [ ] Rust learning resources
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
