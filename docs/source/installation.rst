.. _installation:

Installation
============

``dask-sql`` can be installed via ``conda`` (preferred) or ``pip`` - or in a development environment.

You can continue with the :ref:`quickstart` after the installation.

With ``conda``
--------------

Create a new conda environment or use your already present environment:

.. code-block:: bash

    conda create -n dask-sql
    conda activate dask-sql

Install the package from the ``conda-forge`` channel:

.. code-block:: bash

    conda install dask-sql -c conda-forge

GPU support
^^^^^^^^^^^^^^^^^^^^^^^^

- GPU support is currently tied to the `RAPIDS <https://rapids.ai/>`_  libraries.
- It generally requires the latest `cuDF/Dask-cuDF <https://docs.rapids.ai/api/cudf/legacy/10min.html>`_ nightlies.

Create a new conda environment or use an existing one to install RAPIDS with the chosen methods and packages. 
More details can be found on the `RAPIDS Getting Started <https://rapids.ai/start.html>`_ page, but as an example:

.. code-block:: bash

    conda create --name rapids-env -c rapidsai-nightly -c nvidia -c conda-forge \
        cudf=22.10 dask-cudf=22.10 ucx-py ucx-proc=*=gpu python=3.9 cudatoolkit=11.5
    conda activate rapids-env

Install the stable package from the ``conda-forge`` channel:

.. code-block:: bash

    conda install -c conda-forge dask-sql

Or the latest nightly from the ``dask`` channel (currently only available for Linux-based operating systems):

.. code-block:: bash

    conda install -c dask/label/dev dask-sql


With ``pip``
------------

.. code-block:: bash

    pip install dask-sql

For development
---------------

If you want to have the newest (unreleased) ``dask-sql`` version or if you plan to do development on ``dask-sql``, you can also install the package from sources.

.. code-block:: bash

    git clone https://github.com/dask-contrib/dask-sql.git

Create a new conda environment and install the development environment:

.. code-block:: bash

    conda env create -f continuous_integration/environment-3.9-dev.yaml

It is not recommended to use ``pip`` instead of ``conda``.

After that, you can install the package in development mode

.. code-block:: bash

    pip install -e ".[dev]"

To compile the Rust code (at the beginning or after changes), run

.. code-block:: bash

    python setup.py install

You can run the tests (after installation) with

.. code-block:: bash

    pytest tests

GPU-specific tests require additional dependencies specified in `continuous_integration/gpuci/environment.yaml`:

.. code-block:: bash

    conda env create -n dask-sql-gpuci -f continuous_integration/gpuci/environment.yaml

GPU-specific tests can be run with

.. code-block:: bash

    pytest tests -m gpu --rungpu

This repository uses pre-commit hooks. To install them, call

.. code-block:: bash

    pre-commit install
