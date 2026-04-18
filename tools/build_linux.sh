#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${REPO_ROOT}/build"
BUILD_TYPE="Debug"
JOBS="${JOBS:-}"
ROCKSDB_SOURCE_DIR=""
ROCKSDB_REUSE_BUILD_DIR=""
BUNDLE_DIR="${REPO_ROOT}/third_party/rocksdb/linux-x86_64"
GTEST_DIR="${REPO_ROOT}/third_party/googletest"
SKIP_TESTS=0
FORCE_BUNDLE_REFRESH=0
EXPORT_COMPILE_COMMANDS=1

usage() {
  cat <<'EOF'
Usage:
  tools/build_linux.sh [--build-dir DIR] [--build-type TYPE] [--jobs N]
                       [--rocksdb-source-dir DIR]
                       [--rocksdb-reuse-build-dir DIR]
                       [--force-bundle-refresh]
                       [--no-compile-commands]
                       [--skip-tests]

Defaults:
  --build-dir build
  --build-type Debug
  compile_commands.json exported
  tests enabled

Examples:
  ./tools/build_linux.sh
  ./tools/build_linux.sh --rocksdb-source-dir /path/to/rocksdb
  ./tools/build_linux.sh --rocksdb-source-dir /path/to/rocksdb \
    --rocksdb-reuse-build-dir /path/to/rocksdb/build-minikv
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-dir)
      BUILD_DIR="$2"
      shift 2
      ;;
    --build-type)
      BUILD_TYPE="$2"
      shift 2
      ;;
    --jobs)
      JOBS="$2"
      shift 2
      ;;
    --rocksdb-source-dir)
      ROCKSDB_SOURCE_DIR="$2"
      shift 2
      ;;
    --rocksdb-reuse-build-dir)
      ROCKSDB_REUSE_BUILD_DIR="$2"
      shift 2
      ;;
    --force-bundle-refresh)
      FORCE_BUNDLE_REFRESH=1
      shift
      ;;
    --skip-tests)
      SKIP_TESTS=1
      shift
      ;;
    --no-compile-commands)
      EXPORT_COMPILE_COMMANDS=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "${JOBS}" ]]; then
  if command -v nproc >/dev/null 2>&1; then
    JOBS="$(nproc)"
  else
    JOBS="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 8)"
  fi
fi

if [[ -n "${ROCKSDB_SOURCE_DIR}" ]]; then
  sync_args=(
    --rocksdb-source-dir "${ROCKSDB_SOURCE_DIR}"
    --bundle-dir "${BUNDLE_DIR}"
    --jobs "${JOBS}"
  )
  if [[ -n "${ROCKSDB_REUSE_BUILD_DIR}" ]]; then
    sync_args+=(--reuse-build-dir "${ROCKSDB_REUSE_BUILD_DIR}")
  fi
  if [[ "${FORCE_BUNDLE_REFRESH}" -eq 1 ]]; then
    sync_args+=(--force)
  fi
  "${REPO_ROOT}/tools/sync_rocksdb_bundle.sh" "${sync_args[@]}"
fi

bundle_include="${BUNDLE_DIR}/include/rocksdb/db.h"
bundle_lib="${BUNDLE_DIR}/lib/librocksdb.so"
bundle_manifest="${BUNDLE_DIR}/BUNDLE_INFO.env"
bundle_ready=0
if [[ -f "${bundle_include}" && -f "${bundle_lib}" ]]; then
  bundle_ready=1
fi

if [[ "${bundle_ready}" -eq 0 && -f "${bundle_manifest}" && -z "${ROCKSDB_SOURCE_DIR}" ]]; then
  cat >&2 <<EOF
bundled RocksDB is incomplete in ${BUNDLE_DIR}
expected:
  ${bundle_include}
  ${bundle_lib}

This build entrypoint is intended to work offline. Refresh the committed bundle
with ./tools/sync_rocksdb_bundle.sh from a local RocksDB checkout and commit the
resulting lib/ files together with BUNDLE_INFO.env.
EOF
  exit 1
fi

cmake_args=(
  -S "${REPO_ROOT}"
  -B "${BUILD_DIR}"
  -DCMAKE_BUILD_TYPE="${BUILD_TYPE}"
  "-DCMAKE_EXPORT_COMPILE_COMMANDS=$( (( EXPORT_COMPILE_COMMANDS )) && echo ON || echo OFF )"
)

if [[ "${bundle_ready}" -eq 1 ]]; then
  cmake_args+=(
    "-DMINIKV_USE_BUNDLED_ROCKSDB=ON"
    "-DMINIKV_ROCKSDB_BUNDLE_DIR=${BUNDLE_DIR}"
    "-DMINIKV_FETCH_DEPS=OFF"
  )
elif [[ -n "${ROCKSDB_SOURCE_DIR}" ]]; then
  cmake_args+=("-DMINIKV_ROCKSDB_SOURCE_DIR=${ROCKSDB_SOURCE_DIR}")
fi

if [[ -f "${GTEST_DIR}/CMakeLists.txt" ]]; then
  cmake_args+=(
    "-DMINIKV_GTEST_SOURCE_DIR=${GTEST_DIR}"
    "-DMINIKV_FETCH_GTEST=OFF"
  )
fi

cmake "${cmake_args[@]}"

if [[ "${EXPORT_COMPILE_COMMANDS}" -eq 1 && -f "${BUILD_DIR}/compile_commands.json" ]]; then
  if [[ "${BUILD_DIR}" == "${REPO_ROOT}"/* ]]; then
    compile_commands_link_target="${BUILD_DIR#${REPO_ROOT}/}/compile_commands.json"
    ln -sfn "${compile_commands_link_target}" "${REPO_ROOT}/compile_commands.json"
  fi
fi

cmake --build "${BUILD_DIR}" --parallel "${JOBS}"

if [[ "${SKIP_TESTS}" -eq 0 ]]; then
  tests=(
    minikv_cmd_test
    minikv_command_registry_test
    minikv_module_exports_test
    minikv_module_keyspace_test
    minikv_module_iterator_test
    minikv_background_executor_test
    minikv_hash_module_test
    minikv_list_module_test
    minikv_set_module_test
    minikv_hash_bridge_test
    minikv_hash_observer_test
    minikv_module_manager_test
    minikv_network_test
    minikv_reply_encode_test
    minikv_scheduler_test
    minikv_snapshot_test
  )

  for test_bin in "${tests[@]}"; do
    "${BUILD_DIR}/${test_bin}"
  done
fi

echo "build dir: ${BUILD_DIR}"
if [[ "${EXPORT_COMPILE_COMMANDS}" -eq 1 && -f "${BUILD_DIR}/compile_commands.json" ]]; then
  echo "compile commands: ${BUILD_DIR}/compile_commands.json"
fi
