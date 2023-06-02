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

################################################################################
# SETUP - Check environment
################################################################################

gpuci_logger "Check environment variables"
env

gpuci_logger "Check GPU usage"
nvidia-smi

gpuci_logger "Activate conda env"
. /opt/conda/etc/profile.d/conda.sh
conda activate dask_sql

gpuci_logger "Install awscli"
gpuci_mamba_retry install -y -c conda-forge awscli

gpuci_logger "Download parquet dataset"
gpuci_retry aws s3 cp --only-show-errors "${DASK_SQL_BUCKET_NAME}parquet_2gb_sorted/" tests/unit/data/ --recursive

gpuci_logger "Download query files"
gpuci_retry aws s3 cp --only-show-errors "${DASK_SQL_BUCKET_NAME}queries/" tests/unit/queries/ --recursive

# TODO: source install once dask/distributed are unpinned by dask-cuda
# gpuci_logger "Install dask"
# python -m pip install git+https://github.com/dask/dask
# gpuci_logger "Install distributed"
# python -m pip install git+https://github.com/dask/distributed

gpuci_logger "Install dask-sql"
pip install -e . -vv

gpuci_logger "Check Python version"
python --version

gpuci_logger "Check conda environment"
conda info
conda config --show-sources
conda list --show-channel-urls

gpuci_logger "Python py.test for dask-sql"
py.test $WORKSPACE -n 4 -v -m gpu --runqueries --rungpu --junitxml="$WORKSPACE/junit-dask-sql.xml" --cov-config="$WORKSPACE/.coveragerc" --cov=dask_sql --cov-report=xml:"$WORKSPACE/dask-sql-coverage.xml" --cov-report term
