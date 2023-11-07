#!/bin/bash

set -ex

# See https://github.com/conda-forge/rust-feedstock/blob/master/recipe/build.sh for cc env explanation
if [ "$c_compiler" = gcc ] ; then
    case "$target_platform" in
        linux-64) rust_env_arch=X86_64_UNKNOWN_LINUX_GNU ;;
        linux-aarch64) rust_env_arch=AARCH64_UNKNOWN_LINUX_GNU ;;
        linux-ppc64le) rust_env_arch=POWERPC64LE_UNKNOWN_LINUX_GNU ;;
        *) echo "unknown target_platform $target_platform" ; exit 1 ;;
    esac

    export CARGO_TARGET_${rust_env_arch}_LINKER=$CC
fi

declare -a _xtra_maturin_args

mkdir -p $SRC_DIR/.cargo

if [ "$target_platform" = "osx-64" ] ; then
    cat <<EOF >> $SRC_DIR/.cargo/config
[target.x86_64-apple-darwin]
linker = "$CC"
rustflags = [
  "-C", "link-arg=-undefined",
  "-C", "link-arg=dynamic_lookup",
]

EOF

    _xtra_maturin_args+=(--target=x86_64-apple-darwin)

elif [ "$target_platform" = "osx-arm64" ] ; then
    cat <<EOF >> $SRC_DIR/.cargo/config
# Required for intermediate codegen stuff
[target.x86_64-apple-darwin]
linker = "$CC_FOR_BUILD"

# Required for final binary artifacts for target
[target.aarch64-apple-darwin]
linker = "$CC"
rustflags = [
  "-C", "link-arg=-undefined",
  "-C", "link-arg=dynamic_lookup",
]

EOF
    _xtra_maturin_args+=(--target=aarch64-apple-darwin)

    # This variable must be set to the directory containing the target's libpython DSO
    export PYO3_CROSS_LIB_DIR=$PREFIX/lib

    # xref: https://github.com/PyO3/pyo3/commit/7beb2720
    export PYO3_PYTHON_VERSION=${PY_VER}

    # xref: https://github.com/conda-forge/python-feedstock/issues/621
    sed -i.bak 's,aarch64,arm64,g' $BUILD_PREFIX/venv/lib/os-patch.py
    sed -i.bak 's,aarch64,arm64,g' $BUILD_PREFIX/venv/lib/platform-patch.py
fi

maturin build -vv -j "${CPU_COUNT}" --release --strip --manylinux off --interpreter="${PYTHON}" "${_xtra_maturin_args[@]}"

"${PYTHON}" -m pip install $SRC_DIR/target/wheels/dask_sql*.whl --no-deps -vv
