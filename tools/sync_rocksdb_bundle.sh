#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUNDLE_DIR="${REPO_ROOT}/third_party/rocksdb/linux-x86_64"
ROCKSDB_SOURCE_DIR=""
REUSE_BUILD_DIR=""
BUILD_DIR="/tmp/minikv-rocksdb-bundle-build"
JOBS="${JOBS:-}"
FORCE=0
STATUS_ONLY=0

usage() {
  cat <<'EOF'
Usage:
  tools/sync_rocksdb_bundle.sh --rocksdb-source-dir DIR [options]
  tools/sync_rocksdb_bundle.sh --status [--rocksdb-source-dir DIR]

Options:
  --rocksdb-source-dir DIR   Local RocksDB source checkout used for commit detection.
  --bundle-dir DIR           Bundle output directory. Default: third_party/rocksdb/linux-x86_64
  --build-dir DIR            Temporary out-of-tree RocksDB build directory.
  --reuse-build-dir DIR      Reuse an existing RocksDB shared-library build instead of rebuilding.
  --jobs N                   Build parallelism.
  --force                    Refresh the bundle even if the source commit matches.
  --status                   Print bundle metadata and whether a refresh is needed.

Examples:
  ./tools/sync_rocksdb_bundle.sh --status
  ./tools/sync_rocksdb_bundle.sh --rocksdb-source-dir /workspace/projects/OpenSource/rocksdb
  ./tools/sync_rocksdb_bundle.sh --rocksdb-source-dir /workspace/projects/OpenSource/rocksdb \
    --reuse-build-dir /workspace/projects/OpenSource/rocksdb/build-minikv
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rocksdb-source-dir)
      ROCKSDB_SOURCE_DIR="$2"
      shift 2
      ;;
    --bundle-dir)
      BUNDLE_DIR="$2"
      shift 2
      ;;
    --build-dir)
      BUILD_DIR="$2"
      shift 2
      ;;
    --reuse-build-dir)
      REUSE_BUILD_DIR="$2"
      shift 2
      ;;
    --jobs)
      JOBS="$2"
      shift 2
      ;;
    --force)
      FORCE=1
      shift
      ;;
    --status)
      STATUS_ONLY=1
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

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "x86_64" ]]; then
  echo "this script currently refreshes only the linux-x86_64 RocksDB bundle" >&2
  exit 1
fi

bundle_manifest="${BUNDLE_DIR}/BUNDLE_INFO.env"
bundle_include="${BUNDLE_DIR}/include/rocksdb"
bundle_lib_dir="${BUNDLE_DIR}/lib"
bundle_link="${bundle_lib_dir}/librocksdb.so"

bundle_commit=""
bundle_describe=""
bundle_lib_name=""
if [[ -f "${bundle_manifest}" ]]; then
  # shellcheck disable=SC1090
  source "${bundle_manifest}"
  bundle_commit="${ROCKSDB_SOURCE_COMMIT:-}"
  bundle_describe="${ROCKSDB_SOURCE_DESCRIBE:-}"
  bundle_lib_name="${ROCKSDB_LIBRARY_REALNAME:-}"
fi

source_commit=""
source_describe=""
if [[ -n "${ROCKSDB_SOURCE_DIR}" ]]; then
  if [[ ! -d "${ROCKSDB_SOURCE_DIR}/.git" ]]; then
    echo "rocksdb source dir is not a git checkout: ${ROCKSDB_SOURCE_DIR}" >&2
    exit 1
  fi
  source_commit="$(git -C "${ROCKSDB_SOURCE_DIR}" rev-parse HEAD)"
  source_describe="$(git -C "${ROCKSDB_SOURCE_DIR}" describe --tags --always --dirty)"
fi

bundle_ready=0
if [[ -n "${bundle_commit}" && -f "${bundle_link}" && -d "${bundle_include}" ]]; then
  bundle_ready=1
fi

needs_refresh=0
if [[ "${bundle_ready}" -eq 0 ]]; then
  needs_refresh=1
elif [[ -n "${source_commit}" && "${bundle_commit}" != "${source_commit}" ]]; then
  needs_refresh=1
elif [[ "${FORCE}" -eq 1 ]]; then
  needs_refresh=1
fi

if [[ "${STATUS_ONLY}" -eq 1 ]]; then
  echo "bundle_dir=${BUNDLE_DIR}"
  if [[ "${bundle_ready}" -eq 1 ]]; then
    echo "bundle_status=present"
    echo "bundle_commit=${bundle_commit}"
    echo "bundle_describe=${bundle_describe}"
    echo "bundle_library=${bundle_lib_name}"
  else
    echo "bundle_status=missing"
  fi
  if [[ -n "${source_commit}" ]]; then
    echo "source_commit=${source_commit}"
    echo "source_describe=${source_describe}"
  fi
  if [[ "${needs_refresh}" -eq 1 ]]; then
    echo "refresh=needed"
  else
    echo "refresh=not-needed"
  fi
  exit 0
fi

if [[ -z "${ROCKSDB_SOURCE_DIR}" ]]; then
  echo "--rocksdb-source-dir is required unless --status is used" >&2
  exit 1
fi

if [[ "${needs_refresh}" -eq 0 ]]; then
  echo "rocksdb bundle is already up to date: ${source_commit}"
  exit 0
fi

resolve_real_library() {
  local build_root="$1"
  if [[ -L "${build_root}/librocksdb.so" || -f "${build_root}/librocksdb.so" ]]; then
    readlink -f "${build_root}/librocksdb.so"
    return 0
  fi

  local candidate
  candidate="$(find "${build_root}" -maxdepth 1 -type f -name 'librocksdb.so*' | sort | head -n 1 || true)"
  if [[ -z "${candidate}" ]]; then
    return 1
  fi
  printf '%s\n' "${candidate}"
}

resolve_soname_link() {
  local build_root="$1"
  if [[ -L "${build_root}/librocksdb.so" ]]; then
    basename "$(readlink "${build_root}/librocksdb.so")"
    return 0
  fi
  printf '%s\n' "librocksdb.so"
}

refresh_bundle_headers() {
  local source_dir="$1"
  local destination_dir="$2"
  local temp_root="${destination_dir}.tmp.$$"

  rm -rf "${temp_root}"
  mkdir -p "$(dirname "${temp_root}")"
  cp -R "${source_dir}" "${temp_root}"
  rm -rf "${destination_dir}"
  mv "${temp_root}" "${destination_dir}"
}

if [[ -n "${REUSE_BUILD_DIR}" ]]; then
  real_lib="$(resolve_real_library "${REUSE_BUILD_DIR}")"
  soname_link="$(resolve_soname_link "${REUSE_BUILD_DIR}")"
else
  cmake -S "${ROCKSDB_SOURCE_DIR}" -B "${BUILD_DIR}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DROCKSDB_BUILD_SHARED=ON \
    -DWITH_MINIKV=OFF \
    -DWITH_TESTS=OFF \
    -DWITH_TOOLS=OFF \
    -DWITH_CORE_TOOLS=OFF \
    -DWITH_BENCHMARK_TOOLS=OFF \
    -DWITH_GFLAGS=OFF \
    -DWITH_LIBURING=OFF \
    -DWITH_SNAPPY=OFF \
    -DWITH_LZ4=OFF \
    -DWITH_ZLIB=OFF \
    -DWITH_ZSTD=OFF \
    -DWITH_BZ2=OFF \
    -DWITH_JEMALLOC=OFF
  cmake --build "${BUILD_DIR}" --target rocksdb-shared --parallel "${JOBS}"
  real_lib="$(resolve_real_library "${BUILD_DIR}")"
  soname_link="$(resolve_soname_link "${BUILD_DIR}")"
fi

real_lib_name="$(basename "${real_lib}")"

mkdir -p "${bundle_lib_dir}" "${BUNDLE_DIR}/include"
refresh_bundle_headers "${ROCKSDB_SOURCE_DIR}/include/rocksdb" "${bundle_include}"
cp "${real_lib}" "${bundle_lib_dir}/${real_lib_name}"
if command -v strip >/dev/null 2>&1; then
  strip --strip-unneeded "${bundle_lib_dir}/${real_lib_name}"
fi
ln -sfn "${real_lib_name}" "${bundle_lib_dir}/${soname_link}"
ln -sfn "${soname_link}" "${bundle_link}"

cat > "${bundle_manifest}" <<EOF
ROCKSDB_SOURCE_COMMIT=${source_commit}
ROCKSDB_SOURCE_DESCRIBE=${source_describe}
ROCKSDB_LIBRARY_REALNAME=${real_lib_name}
ROCKSDB_LIBRARY_SONAME=${soname_link}
ROCKSDB_BUNDLE_KIND=shared-stripped
ROCKSDB_BUNDLE_PLATFORM=linux-x86_64
ROCKSDB_BUNDLE_CREATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
EOF

echo "updated rocksdb bundle in ${BUNDLE_DIR}"
