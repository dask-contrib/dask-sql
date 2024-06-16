##################################################
# dask-sql GPU build and test script for CI      #
##################################################
set -e
NUMARGS=$#
ARGS=$*

# Arg parsing function
function hasArg {
    (( ${NUMARGS} != 0 )) && (echo " ${ARGS} " | grep -q " $1 ")
}

# Set path and build parallel level
export PATH=/opt/cargo/bin:/opt/conda/bin:/usr/local/cuda/bin:$PATH
export PARALLEL_LEVEL=${PARALLEL_LEVEL:-4}

# Set home to the job's workspace
export HOME="$WORKSPACE"

# Switch to project root; also root of repo checkout
cd "$WORKSPACE"

# Determine CUDA release version
export CUDA_REL=${CUDA_VERSION%.*}

# TODO: remove once RAPIDS 24.08 has support for query planning
export DASK_DATAFRAME__QUERY_PLANNING=false

################################################################################
# SETUP - Check environment
################################################################################

rapids-logger "Check environment variables"
env

rapids-logger "Check GPU usage"
nvidia-smi

rapids-logger "Activate conda env"
. /opt/conda/etc/profile.d/conda.sh
conda activate dask_sql

rapids-logger "Update conda env"
gpuci_mamba_retry env update -n dask_sql -f continuous_integration/gpuci/environment-${PYTHON_VER}.yaml

rapids-logger "Install awscli"
gpuci_mamba_retry install -y -c conda-forge awscli

rapids-logger "Download parquet dataset"
rapids-retry aws s3 cp --only-show-errors "${DASK_SQL_BUCKET_NAME}parquet_2gb_sorted/" tests/unit/data/ --recursive

rapids-logger "Download query files"
rapids-retry aws s3 cp --only-show-errors "${DASK_SQL_BUCKET_NAME}queries/" tests/unit/queries/ --recursive

rapids-logger "Install dask-sql"
pip install -e . -vv

rapids-logger "Check Python version"
python --version

rapids-logger "Check conda environment"
conda info
conda config --show-sources
conda list --show-channel-urls

rapids-logger "Python py.test for dask-sql"
py.test $WORKSPACE -n $PARALLEL_LEVEL -v -m gpu --runqueries --rungpu --junitxml="$WORKSPACE/junit-dask-sql.xml" --cov-config="$WORKSPACE/.coveragerc" --cov=dask_sql --cov-report=xml:"$WORKSPACE/dask-sql-coverage.xml" --cov-report term
