include(FetchContent)

find_package(Threads REQUIRED)

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
  if(MINIKV_USE_BUNDLED_ROCKSDB AND
     CMAKE_SYSTEM_NAME STREQUAL "Linux" AND
     CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64)$")
    set(minikv_bundle_include "${MINIKV_ROCKSDB_BUNDLE_DIR}/include")
    set(minikv_bundle_lib "${MINIKV_ROCKSDB_BUNDLE_DIR}/lib/librocksdb.so")
    if(EXISTS "${minikv_bundle_include}/rocksdb/db.h" AND
       EXISTS "${minikv_bundle_lib}")
      add_library(minikv_rocksdb_bundle SHARED IMPORTED GLOBAL)
      set_target_properties(minikv_rocksdb_bundle PROPERTIES
        IMPORTED_LOCATION "${minikv_bundle_lib}"
        INTERFACE_INCLUDE_DIRECTORIES "${minikv_bundle_include}")
      set(MINIKV_ROCKSDB_TARGET minikv_rocksdb_bundle PARENT_SCOPE)
      set(MINIKV_ROCKSDB_RUNTIME_RPATH
          "\$ORIGIN/../third_party/rocksdb/linux-x86_64/lib" PARENT_SCOPE)
      message(STATUS "Using bundled RocksDB from ${MINIKV_ROCKSDB_BUNDLE_DIR}")
      return()
    endif()

    message(STATUS
      "Bundled RocksDB not found in ${MINIKV_ROCKSDB_BUNDLE_DIR}; "
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
    FetchContent_Declare(
      rocksdb
      GIT_REPOSITORY https://github.com/facebook/rocksdb.git
      GIT_TAG ${MINIKV_ROCKSDB_TAG}
      GIT_SHALLOW TRUE)
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

  if(NOT MINIKV_FETCH_GTEST)
    message(FATAL_ERROR
      "BUILD_TESTING requires MINIKV_FETCH_GTEST=ON so googletest can be fetched.")
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
