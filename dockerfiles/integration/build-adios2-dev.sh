#!/bin/bash
set -euo pipefail

source_dir=${ADIOS2_DEV_SOURCE_DIR:-/workspace/ADIOS2}
build_dir=${ADIOS2_DEV_BUILD_DIR:-/workspace/ADIOS2-build}
install_prefix=${ADIOS2_DEV_INSTALL_PREFIX:-/opt/adios2-dev}
build_type=${ADIOS2_DEV_BUILD_TYPE:-RelWithDebInfo}
build_testing=${ADIOS2_DEV_BUILD_TESTING:-OFF}
build_jobs=${BUILD_JOBS:-8}

if [[ ! -f "${source_dir}/CMakeLists.txt" ]]; then
    echo "ADIOS2 source not found at ${source_dir}" >&2
    echo "Set ADIOS2_DEV_SOURCE to a local ADIOS2 checkout before starting Compose." >&2
    exit 2
fi

cmake \
    -S "${source_dir}" \
    -B "${build_dir}" \
    -G Ninja \
    -DADIOS2_BUILD_EXAMPLES=OFF \
    -DADIOS2_USE_AWSSDK=ON \
    -DADIOS2_USE_CURL=ON \
    -DADIOS2_USE_Campaign=ON \
    -DADIOS2_USE_Fortran=OFF \
    -DADIOS2_USE_HDF5=ON \
    -DADIOS2_USE_MPI=OFF \
    -DADIOS2_USE_Python=ON \
    -DADIOS2_USE_Sodium=ON \
    -DADIOS2_USE_XRootD=ON \
    -DBUILD_TESTING="${build_testing}" \
    -DCMAKE_BUILD_TYPE="${build_type}" \
    -DCMAKE_INSTALL_PREFIX="${install_prefix}" \
    -DCMAKE_INSTALL_PYTHONDIR="${install_prefix}/lib/python3.13/site-packages" \
    -DPython_EXECUTABLE=/usr/local/bin/python \
    "$@"

cmake --build "${build_dir}" --parallel "${build_jobs}"
cmake --install "${build_dir}"

python -c "import adios2; print(f'Using ADIOS2 {adios2.__version__} from {adios2.__file__}')"
