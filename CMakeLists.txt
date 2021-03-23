cmake_minimum_required (VERSION 3.13)
if (POLICY CMP0068)
    cmake_policy(SET CMP0068 NEW)
endif ()
# Define file(GENERATE) behavior for relative paths.
if (POLICY CMP0070)
    cmake_policy(SET CMP0070 NEW)
endif ()
# target_sources: use absolute path for INTERFACE_SOURCES.
if (POLICY CMP0076)
    cmake_policy(SET CMP0076 NEW)
endif ()
# SWIG: use standard target name.
if (POLICY CMP0078)
    cmake_policy(SET CMP0078 NEW)
endif ()
# SWIG: use SWIG_MODULE_NAME property.
if (POLICY CMP0086)
    cmake_policy(SET CMP0086 NEW)
endif ()

project(rtidb)

if (CMAKE_BUILD_TYPE STREQUAL "")
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif ()
message (STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
set(FEDB_VERSION_MAJOR 2)
set(FEDB_VERSION_MEDIUM 1)
set(FEDB_VERSION_MINOR 0)
set(FEDB_COMMIT_ID 0)
set(FESQL_COMMIT_ID 0)

function(get_commitid CODE_DIR COMMIT_ID)
    find_package(Git REQUIRED)
    execute_process(
        COMMAND ${GIT_EXECUTABLE} log -1 --pretty=format:%h
        OUTPUT_VARIABLE COMMIT_ID_TMP
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
        WORKING_DIRECTORY
        ${CODE_DIR}
    )
    set("${COMMIT_ID}" ${COMMIT_ID_TMP} PARENT_SCOPE)
endfunction()

get_commitid(${PROJECT_SOURCE_DIR} FEDB_COMMIT_ID)
get_commitid(${PROJECT_SOURCE_DIR}/fesql FESQL_COMMIT_ID)
message(STATUS "COMMIT_ID : ${FEDB_COMMIT_ID}.${FESQL_COMMIT_ID}")
configure_file (
  "${PROJECT_SOURCE_DIR}/src/version.h.in"
  "${PROJECT_SOURCE_DIR}/src/version.h"
)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
add_definitions('-g')
add_definitions('-Wall')
if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    add_definitions(-Wreorder)
    add_definitions(-Wno-unused-private-field)
    add_definitions(-Wno-unused-function)
    add_definitions(-Wno-overloaded-virtual)
    add_definitions(-Wno-inconsistent-missing-override)
    add_definitions(-Wno-deprecated-declarations)
    add_definitions(-Wno-null-dereference)
    add_definitions(-Wno-deprecated-register)
    add_definitions(-Wno-c++17-extensions)
    add_definitions(-Wno-defaulted-function-deleted)
    add_definitions(-Wno-pessimizing-move)
    add_definitions(-Wno-unused-value)
    add_definitions(-Wno-\#pragma-messages)
endif ()
add_definitions(-DHAVE_STDBOOL_H=1)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
option(TESTING_ENABLE "Enable test" OFF)
option(TCMALLOC_ENABLE "Enable TCMALLOC" ON)
option(SQL_PYSDK_ENABLE "Enable sql pysdk" OFF)
option(SQL_JAVASDK_ENABLE "Enable sql javasdk" OFF)
option(MAC_TABLET_ENABLE "Enable Table on Mac OS" OFF)
message (STATUS "MAC_TABLET_ENABLE: ${MAC_TABLET_ENABLE}")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
if (MAC_TABLET_ENABLE)
    add_compile_definitions(__mac_tablet__=1)
endif ()
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(OS_LIB ${CMAKE_THREAD_LIBS_INIT} rt)
    set(BRPC_LIBS brpc protobuf glog gflags unwind lzma ssl crypto leveldb z snappy dl pthread ${OS_LIB})
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(OS_LIB 
            ${CMAKE_THREAD_LIBS_INIT}
            "-framework CoreFoundation"
            "-framework CoreGraphics"
            "-framework CoreData"
            "-framework CoreText"
            "-framework Security"
            "-framework Foundation"
            "-Wl,-U,_MallocExtension_ReleaseFreeMemory"
            "-Wl,-U,_ProfilerStart"
            "-Wl,-U,_ProfilerStop")
    set(BRPC_LIBS brpc protobuf glog gflags ssl crypto leveldb z snappy dl pthread ${OS_LIB})
endif ()
set(VM_LIBS fesql_core fesql_sdk boost_filesystem boost_date_time boost_regex)
set(LLVM_DIR "${PROJECT_SOURCE_DIR}/thirdparty/lib/cmake/llvm")
find_package(LLVM REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
llvm_map_components_to_libnames(LLVM_LIBS support core orcjit nativecodegen)
add_definitions(${LLVM_DEFINITIONS})
find_package(SWIG REQUIRED)
include(UseSWIG)
if (TCMALLOC_ENABLE)
    set(CMAKE_EXE_LINKER_FLAGS "-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free ${CMAKE_EXE_LINKER_FLAGS}")
endif()

if(POLICY CMP0086)
  cmake_policy(SET CMP0086 NEW)
endif()

configure_file(
    "${PROJECT_SOURCE_DIR}/src/config.h.in"
    "${PROJECT_SOURCE_DIR}/src/config.h")

include_directories(${PROJECT_SOURCE_DIR}/thirdparty/include ${PROJECT_SOURCE_DIR}/fesql/src 
    ${PROJECT_SOURCE_DIR}/fesql/build/src)

link_directories(${PROJECT_SOURCE_DIR}/thirdparty/lib 
    ${PROJECT_SOURCE_DIR}/thirdparty/lib64
    ${PROJECT_SOURCE_DIR}/fesql/build/release/lib)

add_subdirectory(src)
add_custom_target(rtidbpy
COMMAND make
WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/swig
)
