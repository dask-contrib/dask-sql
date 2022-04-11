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

Experimental GPU support
^^^^^^^^^^^^^^^^^^^^^^^^

- GPU support is currently tied to the `RAPIDS <https://rapids.ai/>`_  libraries.
- It is in development and generally requires the latest `cuDF/Dask-cuDF <https://docs.rapids.ai/api/cudf/legacy/10min.html>`_ nightlies.
- It is experimental, so users should expect some bugs or undefined behavior.

Create a new conda environment or use an existing one to install RAPIDS with the chosen methods and packages:

.. code-block:: bash

    conda create --name rapids-env -c rapidsai-nightly -c nvidia -c conda-forge \
        cudf=22.02 dask-cudf=22.02 ucx-py ucx-proc=*=gpu python=3.8 cudatoolkit=11.2
    conda activate rapids-env

Install the stable package from the ``conda-forge`` channel:

.. code-block:: bash

    conda install -c conda-forge dask-sql

Or the latest nightly from the ``dask`` channel (currently only available for Linux-based operating systems):

.. code-block:: bash

    conda install -c dask/label/dev dask-sql


With ``pip``
------------

``dask-sql`` needs Java for the parsing of the SQL queries.
Before installation, make sure you have a running java installation with version >= 8.

To test if you have Java properly installed and set up, run

.. code-block:: bash

    $ java -version
    openjdk version "1.8.0_152-release"
    OpenJDK Runtime Environment (build 1.8.0_152-release-1056-b12)
    OpenJDK 64-Bit Server VM (build 25.152-b12, mixed mode)

After installing Java, you can install the package with

.. code-block:: bash

    pip install dask-sql

For development
---------------

If you want to have the newest (unreleased) ``dask-sql`` version or if you plan to do development on ``dask-sql``, you can also install the package from sources.

.. code-block:: bash

    git clone https://github.com/dask-contrib/dask-sql.git

Create a new conda environment and install the development environment:

.. code-block:: bash

    conda env create -f continuous_integration/environment-3.9-jdk11-dev.yaml

It is not recommended to use ``pip`` instead of ``conda``.
If you however need to, make sure to have Java (jdk >= 8) and maven installed and correctly setup before continuing.
Have a look into ``environment-3.9-jdk11-dev.yaml`` for the rest of the development environment.

After that, you can install the package in development mode

.. code-block:: bash

    pip install -e ".[dev]"

To compile the Java classes (at the beginning or after changes), run

.. code-block:: bash

    python setup.py build_ext

You can run the tests (after installation) with

.. code-block:: bash

    pytest tests

This repository uses pre-commit hooks. To install them, call

.. code-block:: bash

    pre-commit install
