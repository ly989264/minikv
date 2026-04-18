cmake_minimum_required(VERSION 3.20)

if(NOT DEFINED CMAKE_EXPORT_COMPILE_COMMANDS)
  set(CMAKE_EXPORT_COMPILE_COMMANDS ON CACHE BOOL
      "Generate compile_commands.json for clangd and other tooling")
endif()

project(minikv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(MINIKV_FETCH_DEPS "Fetch RocksDB and googletest during configure" ON)
option(MINIKV_FETCH_GTEST "Fetch googletest during configure when tests are enabled" ON)
option(MINIKV_USE_BUNDLED_ROCKSDB
       "Prefer the committed RocksDB bundle under third_party/rocksdb when available" ON)
set(MINIKV_ROCKSDB_TAG "v11.0.4" CACHE STRING
    "Pinned RocksDB tag used when MINIKV_FETCH_DEPS is enabled")
set(MINIKV_GTEST_TAG "v1.14.0" CACHE STRING
    "Pinned googletest tag used when MINIKV_FETCH_DEPS is enabled")
set(MINIKV_ROCKSDB_SOURCE_DIR "" CACHE PATH
    "Optional local RocksDB source tree used instead of fetching RocksDB")
set(MINIKV_GTEST_SOURCE_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/googletest"
    CACHE PATH "Optional local googletest source tree used instead of fetching googletest")
set(MINIKV_ROCKSDB_BUNDLE_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/rocksdb/linux-x86_64"
    CACHE PATH "Path to a committed RocksDB bundle (headers + shared library)")

include(CTest)
include(GNUInstallDirs)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(Dependencies)

function(minikv_apply_runtime_path target)
  if(MINIKV_ROCKSDB_RUNTIME_RPATH)
    set_target_properties(${target}
      PROPERTIES
        BUILD_RPATH "${MINIKV_ROCKSDB_RUNTIME_RPATH}"
        BUILD_RPATH_USE_ORIGIN ON)
  endif()
endfunction()

set(MINIKV_RUNTIME_HEADERS
  src/runtime/config.h
  src/runtime/minikv.h
  src/network/network_server.h)

set(MINIKV_INTERNAL_HEADERS
  src/execution/command/command_types.h
  src/execution/command/cmd.h
  src/execution/command/cmd_create.h
  src/common/thread_name.h
  src/storage/encoding/key_codec.h
  src/execution/registry/command_registry.h
  src/execution/reply/reply.h
  src/execution/reply/reply_node.h
  src/execution/scheduler/scheduler.h
  src/storage/engine/snapshot.h
  src/storage/engine/storage_engine.h
  src/storage/engine/write_context.h
  src/runtime/module/module.h
  src/runtime/module/background_executor.h
  src/runtime/module/module_manager.h
  src/runtime/module/module_services.h
  src/core/core_module.h
  src/core/key_service.h
  src/core/whole_key_delete_handler.h
  src/types/hash/hash_indexing_bridge.h
  src/types/hash/hash_module.h
  src/types/hash/hash_observer.h
  src/types/hash/hash_types.h
  src/network/resp_parser.h
  src/execution/worker/key_lock_table.h
  src/execution/worker/worker.h)

set(MINIKV_SOURCES
  src/execution/command/cmd.cc
  src/execution/command/cmd_create.cc
  src/common/thread_name.cc
  src/storage/encoding/key_codec.cc
  src/execution/registry/command_registry.cc
  src/execution/reply/reply.cc
  src/execution/scheduler/scheduler.cc
  src/storage/engine/snapshot.cc
  src/storage/engine/storage_engine.cc
  src/storage/engine/write_context.cc
  src/runtime/module/module_manager.cc
  src/runtime/module/background_executor.cc
  src/runtime/module/module_services.cc
  src/core/core_module.cc
  src/core/key_service.cc
  src/types/hash/hash_module.cc
  src/network/network_server.cc
  src/network/resp_parser.cc
  src/execution/worker/key_lock_table.cc
  src/execution/worker/worker.cc
  src/runtime/minikv.cc)

add_library(minikv_core STATIC
  ${MINIKV_RUNTIME_HEADERS}
  ${MINIKV_INTERNAL_HEADERS}
  ${MINIKV_SOURCES})

target_include_directories(minikv_core
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src)

target_compile_features(minikv_core PUBLIC cxx_std_17)
target_link_libraries(minikv_core
  PUBLIC
    ${MINIKV_ROCKSDB_TARGET}
  PRIVATE
    Threads::Threads)

add_executable(minikv_server src/app/main.cc)
target_link_libraries(minikv_server PRIVATE minikv_core)
target_compile_features(minikv_server PRIVATE cxx_std_17)
minikv_apply_runtime_path(minikv_server)

if(BUILD_TESTING)
  function(minikv_add_test target source)
    add_executable(${target} ${source})
    target_link_libraries(${target}
      PRIVATE
        minikv_core
        GTest::gtest
        Threads::Threads)
    target_compile_features(${target} PRIVATE cxx_std_17)
    target_include_directories(${target} PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR}/src)
    minikv_apply_runtime_path(${target})
    add_test(NAME ${target} COMMAND ${target})
  endfunction()

  minikv_add_test(minikv_cmd_test tests/command_test/cmd_test.cc)
  minikv_add_test(minikv_command_registry_test
    tests/command_test/command_registry_test.cc)
  minikv_add_test(minikv_hash_module_test tests/type_test/hash_module_test.cc)
  minikv_add_test(minikv_network_test tests/network_test/server_test.cc)
  minikv_add_test(minikv_reply_encode_test
    tests/network_test/reply_encode_test.cc)
  minikv_add_test(minikv_scheduler_test tests/kernel_test/scheduler_test.cc)
  minikv_add_test(minikv_snapshot_test tests/kernel_test/snapshot_test.cc)
  minikv_add_test(minikv_module_manager_test
    tests/module_test/module_manager_test.cc)
  minikv_add_test(minikv_module_exports_test
    tests/module_test/module_exports_test.cc)
  minikv_add_test(minikv_module_keyspace_test
    tests/module_test/module_keyspace_test.cc)
  minikv_add_test(minikv_module_iterator_test
    tests/module_test/module_iterator_test.cc)
  minikv_add_test(minikv_background_executor_test
    tests/module_test/background_executor_test.cc)
  minikv_add_test(minikv_hash_bridge_test
    tests/module_test/hash_bridge_test.cc)
  minikv_add_test(minikv_hash_observer_test
    tests/type_test/hash_observer_test.cc)
endif()
