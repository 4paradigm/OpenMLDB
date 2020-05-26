cmake_minimum_required (VERSION 2.8)

project(rtidb)

set(CMAKE_BUILD_TYPE RelWithDebInfo)
set(RTIDB_VERSION_MAJOR 1)
set(RTIDB_VERSION_MEDIUM 5)
set(RTIDB_VERSION_MINOR 1)
set(RTIDB_VERSION_BUG 0)
configure_file (
  "${PROJECT_SOURCE_DIR}/src/version.h.in"
  "${PROJECT_SOURCE_DIR}/src/version.h"
)

add_definitions('-g')
add_definitions('-Wall')
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
option(TCMALLOC_ENABLE "Enable TCMALLOC" ON)

option(PZFPGA_ENABLE "Enable pz compression for ssd tables" OFF)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(BRPC_LIBS brpc protobuf glog gflags unwind ssl crypto leveldb z snappy dl pthread rt)
set(VM_LIBS fesql_vm fesql_udf fesql_parser fesql_planner fesql_codegen fesql_storage fesql_codec fesql_proto
    fesql_node fesql_base)
set(LLVM_DIR "${PROJECT_SOURCE_DIR}/thirdparty/lib/cmake/llvm")
find_package(LLVM REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
llvm_map_components_to_libnames(LLVM_LIBS support core orcjit nativecodegen)

if (TCMALLOC_ENABLE)
    set(CMAKE_EXE_LINKER_FLAGS "-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free ${CMAKE_EXE_LINKER_FLAGS}")
endif()

option(LOG_FILE_PATH_ENABLE "Enable LOG FILE PATH ENABLE" ON) 
message("LOG_FILE_PATH_ENABLE option is ${LOG_FILE_PATH_ENABLE}")
if (LOG_FILE_PATH_ENABLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D__FILE__='\"$(subst ${CMAKE_SOURCE_DIR}/,,$(abspath $<))\"' -Wno-builtin-macro-redefined")
endif()
if(POLICY CMP0086)
  cmake_policy(SET CMP0086 NEW)
endif()

configure_file(
    "${PROJECT_SOURCE_DIR}/src/config.h.in"
    "${PROJECT_SOURCE_DIR}/src/config.h")

include_directories(${PROJECT_SOURCE_DIR}/thirdparty/include ${PROJECT_SOURCE_DIR}/fesql/src 
 ${PROJECT_SOURCE_DIR}/fesql/build/src)
if (PZFPGA_ENABLE) 
    add_definitions('-DPZFPGA')
endif()
link_directories(${PROJECT_SOURCE_DIR}/thirdparty/lib ${PROJECT_SOURCE_DIR}/fesql/build/release/lib)
add_subdirectory(src)
add_custom_target(rtidbpy
COMMAND make
WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/swig
)
