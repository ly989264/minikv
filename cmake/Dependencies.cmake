include(FetchContent)

find_package(Threads REQUIRED)

set(MINIKV_SUPPORTED_ROCKSDB_VERSIONS current 5.18.3)

function(minikv_validate_rocksdb_version)
  if(NOT MINIKV_ROCKSDB_VERSION IN_LIST MINIKV_SUPPORTED_ROCKSDB_VERSIONS)
    string(REPLACE ";" ", " minikv_supported_versions
      "${MINIKV_SUPPORTED_ROCKSDB_VERSIONS}")
    message(FATAL_ERROR
      "Unsupported MINIKV_ROCKSDB_VERSION='${MINIKV_ROCKSDB_VERSION}'. "
      "Supported values: ${minikv_supported_versions}.")
  endif()
endfunction()

function(minikv_rocksdb_platform out_var)
  string(TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" minikv_system_processor)
  if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND
     minikv_system_processor MATCHES "^(x86_64|amd64)$")
    set(${out_var} linux-x86_64 PARENT_SCOPE)
    return()
  endif()

  set(${out_var} "" PARENT_SCOPE)
endfunction()

function(minikv_default_rocksdb_bundle_dir out_var)
  minikv_rocksdb_platform(minikv_bundle_platform)
  if(NOT minikv_bundle_platform)
    set(${out_var} "" PARENT_SCOPE)
    return()
  endif()

  set(${out_var}
      "${CMAKE_CURRENT_SOURCE_DIR}/third_party/rocksdb/bundles/${MINIKV_ROCKSDB_VERSION}/${minikv_bundle_platform}"
      PARENT_SCOPE)
endfunction()

function(minikv_default_rocksdb_tag out_var)
  if(MINIKV_ROCKSDB_TAG)
    set(${out_var} "${MINIKV_ROCKSDB_TAG}" PARENT_SCOPE)
  elseif(MINIKV_ROCKSDB_VERSION STREQUAL "5.18.3")
    set(${out_var} "v5.18.3" PARENT_SCOPE)
  else()
    set(${out_var} "v11.0.4" PARENT_SCOPE)
  endif()
endfunction()

function(minikv_validate_versioned_rocksdb_bundle bundle_include bundle_dir)
  set(minikv_manifest "${bundle_dir}/BUNDLE_INFO.env")
  if(MINIKV_ROCKSDB_VERSION STREQUAL "current")
    if(NOT EXISTS "${minikv_manifest}")
      message(FATAL_ERROR
        "The current RocksDB bundle is missing its manifest: "
        "${minikv_manifest}")
    endif()
    return()
  endif()

  if(NOT MINIKV_ROCKSDB_VERSION MATCHES "^([0-9]+)\\.([0-9]+)\\.([0-9]+)$")
    return()
  endif()

  set(minikv_expected_major "${CMAKE_MATCH_1}")
  set(minikv_expected_minor "${CMAKE_MATCH_2}")
  set(minikv_expected_patch "${CMAKE_MATCH_3}")
  set(minikv_version_header "${bundle_include}/rocksdb/version.h")
  if(NOT EXISTS "${minikv_version_header}")
    message(FATAL_ERROR
      "RocksDB bundle '${bundle_dir}' is missing rocksdb/version.h.")
  endif()

  file(READ "${minikv_version_header}" minikv_version_text)
  foreach(component IN ITEMS MAJOR MINOR PATCH)
    set(minikv_component_match "")
    string(REGEX MATCH
      "#define[ \t]+ROCKSDB_${component}[ \t]+([0-9]+)"
      minikv_component_match "${minikv_version_text}")
    if(NOT minikv_component_match)
      message(FATAL_ERROR
        "Could not read ROCKSDB_${component} from ${minikv_version_header}.")
    endif()
    set("minikv_actual_${component}" "${CMAKE_MATCH_1}")
  endforeach()

  if(NOT minikv_actual_MAJOR STREQUAL minikv_expected_major OR
     NOT minikv_actual_MINOR STREQUAL minikv_expected_minor OR
     NOT minikv_actual_PATCH STREQUAL minikv_expected_patch)
    message(FATAL_ERROR
      "RocksDB bundle version mismatch for ${bundle_dir}: expected "
      "${MINIKV_ROCKSDB_VERSION}, found "
      "${minikv_actual_MAJOR}.${minikv_actual_MINOR}.${minikv_actual_PATCH}.")
  endif()
endfunction()

function(minikv_configure_rocksdb_options)
  set(ROCKSDB_BUILD_SHARED OFF CACHE BOOL "" FORCE)
  set(WITH_TESTS OFF CACHE BOOL "" FORCE)
  set(WITH_MINIKV OFF CACHE BOOL "" FORCE)
  set(WITH_TOOLS OFF CACHE BOOL "" FORCE)
  set(WITH_CORE_TOOLS OFF CACHE BOOL "" FORCE)
  set(WITH_BENCHMARK_TOOLS OFF CACHE BOOL "" FORCE)
  set(WITH_GFLAGS OFF CACHE BOOL "" FORCE)
  set(WITH_LIBURING OFF CACHE BOOL "" FORCE)
  set(WITH_SNAPPY OFF CACHE BOOL "" FORCE)
  set(WITH_LZ4 OFF CACHE BOOL "" FORCE)
  set(WITH_ZLIB OFF CACHE BOOL "" FORCE)
  set(WITH_ZSTD OFF CACHE BOOL "" FORCE)
  set(WITH_BZ2 OFF CACHE BOOL "" FORCE)
  set(WITH_JEMALLOC OFF CACHE BOOL "" FORCE)
endfunction()

function(minikv_enable_rocksdb)
  minikv_validate_rocksdb_version()

  if(MINIKV_ROCKSDB_BUNDLE_DIR)
    get_filename_component(minikv_resolved_bundle_dir
      "${MINIKV_ROCKSDB_BUNDLE_DIR}" ABSOLUTE)
  else()
    minikv_default_rocksdb_bundle_dir(minikv_resolved_bundle_dir)
  endif()

  minikv_rocksdb_platform(minikv_bundle_platform)
  if(MINIKV_USE_BUNDLED_ROCKSDB AND minikv_bundle_platform)
    set(minikv_bundle_include "${minikv_resolved_bundle_dir}/include")
    set(minikv_bundle_lib "${minikv_resolved_bundle_dir}/lib/librocksdb.so")
    if(EXISTS "${minikv_bundle_include}/rocksdb/db.h" AND
       EXISTS "${minikv_bundle_lib}")
      minikv_validate_versioned_rocksdb_bundle(
        "${minikv_bundle_include}" "${minikv_resolved_bundle_dir}")
      add_library(minikv_rocksdb_bundle SHARED IMPORTED GLOBAL)
      set_target_properties(minikv_rocksdb_bundle PROPERTIES
        IMPORTED_LOCATION "${minikv_bundle_lib}"
        INTERFACE_INCLUDE_DIRECTORIES "${minikv_bundle_include}")
      file(RELATIVE_PATH minikv_bundle_lib_runtime_path
        "${CMAKE_BINARY_DIR}" "${minikv_resolved_bundle_dir}/lib")
      set(MINIKV_ROCKSDB_TARGET minikv_rocksdb_bundle PARENT_SCOPE)
      set(MINIKV_ROCKSDB_RUNTIME_RPATH
          "\$ORIGIN/${minikv_bundle_lib_runtime_path}" PARENT_SCOPE)
      message(STATUS
        "Using bundled RocksDB '${MINIKV_ROCKSDB_VERSION}' from "
        "${minikv_resolved_bundle_dir}")
      return()
    endif()

    message(STATUS
      "Bundled RocksDB '${MINIKV_ROCKSDB_VERSION}' not found in "
      "${minikv_resolved_bundle_dir}; "
      "falling back to source or FetchContent.")
  endif()

  minikv_configure_rocksdb_options()

  if(MINIKV_ROCKSDB_SOURCE_DIR)
    get_filename_component(minikv_rocksdb_source
      "${MINIKV_ROCKSDB_SOURCE_DIR}" ABSOLUTE)
    if(NOT EXISTS "${minikv_rocksdb_source}/CMakeLists.txt")
      message(FATAL_ERROR
        "MINIKV_ROCKSDB_SOURCE_DIR does not point to a RocksDB source tree: "
        "${minikv_rocksdb_source}")
    endif()
    add_subdirectory("${minikv_rocksdb_source}"
      "${CMAKE_BINARY_DIR}/_deps/rocksdb-build" EXCLUDE_FROM_ALL)
  elseif(MINIKV_FETCH_DEPS)
    minikv_default_rocksdb_tag(minikv_effective_rocksdb_tag)
    FetchContent_Declare(
      rocksdb
      GIT_REPOSITORY https://github.com/facebook/rocksdb.git
      GIT_TAG ${minikv_effective_rocksdb_tag}
      GIT_SHALLOW TRUE)
    message(STATUS
      "Fetching RocksDB '${MINIKV_ROCKSDB_VERSION}' from tag "
      "${minikv_effective_rocksdb_tag}")
    FetchContent_MakeAvailable(rocksdb)
  else()
    message(FATAL_ERROR
      "RocksDB dependency is unavailable. Set MINIKV_FETCH_DEPS=ON or "
      "provide MINIKV_ROCKSDB_SOURCE_DIR.")
  endif()

  if(TARGET rocksdb)
    set(MINIKV_ROCKSDB_TARGET rocksdb PARENT_SCOPE)
    set(MINIKV_ROCKSDB_RUNTIME_RPATH "" PARENT_SCOPE)
  else()
    message(FATAL_ERROR
      "Expected RocksDB static target 'rocksdb' was not created.")
  endif()
endfunction()

function(minikv_enable_gtest)
  if(NOT BUILD_TESTING)
    return()
  endif()

  if(MINIKV_GTEST_SOURCE_DIR)
    get_filename_component(minikv_gtest_source
      "${MINIKV_GTEST_SOURCE_DIR}" ABSOLUTE)
    if(EXISTS "${minikv_gtest_source}/CMakeLists.txt")
      set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
      set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
      add_subdirectory("${minikv_gtest_source}"
        "${CMAKE_BINARY_DIR}/_deps/googletest-build" EXCLUDE_FROM_ALL)
      return()
    elseif(NOT MINIKV_FETCH_GTEST)
      message(FATAL_ERROR
        "MINIKV_GTEST_SOURCE_DIR does not point to a googletest source tree: "
        "${minikv_gtest_source}")
    endif()
  endif()

  if(NOT MINIKV_FETCH_GTEST)
    message(FATAL_ERROR
      "BUILD_TESTING requires a vendored googletest tree or MINIKV_FETCH_GTEST=ON.")
  endif()

  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG ${MINIKV_GTEST_TAG}
    GIT_SHALLOW TRUE)
  FetchContent_MakeAvailable(googletest)
endfunction()

minikv_enable_rocksdb()
minikv_enable_gtest()
