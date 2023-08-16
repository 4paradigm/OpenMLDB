# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.13)

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

project(hybridse)

set(HYBRIDSE_VERSION_MAJOR 0)
set(HYBRIDSE_VERSION_MINOR 4)
set(HYBRIDSE_VERSION_BUG 0)

option(TESTING_ENABLE "Enable Test" ON)
option(HYBRIDSE_TESTING_ENABLE "Enable Hybridse Test" ON)
option(EXAMPLES_TESTING_ENABLE "Enable Examples Test" ON)
option(BENCHMARK_ENABLE "Enable Benchmark" OFF)
option(EXAMPLES_ENABLE "Enable examples" ON)
option(LLVM_EXT_ENABLE "Enable llvm ext sources" OFF)

if (NOT DEFINED CMAKE_PREFIX_PATH)
    set(CMAKE_PREFIX_PATH "${CMAKE_SOURCE_DIR}/.deps/usr")
endif()

if (CMAKE_BUILD_TYPE STREQUAL "")
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif ()
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# llvm dependency
if (NOT DEFINED LLVM_DIR)
    set(LLVM_DIR "${CMAKE_PREFIX_PATH}/lib/cmake/llvm")
endif ()
find_package(LLVM REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")

if (LLVM_EXT_ENABLE)
    llvm_map_components_to_libnames(LLVM_LIBS
            support core orcjit nativecodegen
            mcjit executionengine IntelJITEvents PerfJITEvents object)
else ()
    llvm_map_components_to_libnames(LLVM_LIBS
            support core orcjit nativecodegen)
endif ()
message(STATUS "Using LLVM components: ${LLVM_LIBS}")

find_package(Threads)

set(Boost_NO_BOOST_CMAKE ON)
set(BOOST_ROOT "${CMAKE_PREFIX_PATH}")
find_package(Boost COMPONENTS filesystem date_time regex REQUIRED)
find_package(OpenSSL REQUIRED)

set(Protobuf_USE_STATIC_LIBS ON)
find_package(Protobuf 3.6 EXACT REQUIRED)
message(STATUS "Found Protobuf Libraries: ${Protobuf_LIBRARIES}")

find_library(UNWIND_LIBRARY unwind)
find_library(LEVELDB_LIBRARY leveldb)
find_library(Z_LIBRARY z)
find_library(SNAPPY_LIBRARY snappy)

message(STATUS "Boost libraries ${Boost_LIBRARIES}")
message(STATUS "OpenSSL libraries ${OPENSSL_LIBRARIES}")

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(OS_LIB ${CMAKE_THREAD_LIBS_INIT} rt)
    set(COMMON_LIBS ${Boost_LIBRARIES} ${LEVELDB_LIBRARY} ${Protobuf_LIBRARIES} ${GLOG_LIBRARY} ${UNWIND_LIBRARY} ${Z_LIBRARY} ${SNAPPY_LIBRARY} ${OPENSSL_LIBRARIES} dl)
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
    set(COMMON_LIBS ${Boost_LIBRARIES} ${LEVELDB_LIBRARY} ${Protobuf_LIBRARIES} ${GLOG_LIBRARY} ${Z_LIBRARY} ${SNAPPY_LIBRARY} ${OPENSSL_LIBRARIES} dl)
endif ()

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "9.1")
    # GNU implementation prior to 9.1 requires linking with -lstdc++fs
    list(APPEND COMMON_LIBS stdc++fs)
endif()

configure_file(
        "${PROJECT_SOURCE_DIR}/src/version.h.in"
        "${PROJECT_SOURCE_DIR}/src/hybridse_version.h"
)
if (DEFINED ENV{CI})
  # suppress useless maven log (e.g download log) on CI environment
  set(MAVEN_FLAGS --batch-mode)
endif()

if (TESTING_ENABLE)
    set(CTEST_OUTPUT_ON_FAILURE ON)
    enable_testing()
endif ()

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_definitions('-Wall')
    add_definitions('-fexceptions')
    add_definitions(-Wno-unused-function)
    add_definitions(-Wno-register)
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    add_definitions('-Wall')
    add_definitions('-fexceptions')
    add_definitions(-Wreorder)
    add_definitions(-Wno-register)
    add_definitions(-Wno-unused-private-field)
    add_definitions(-Wno-unused-function)
    add_definitions(-Wno-overloaded-virtual)
    add_definitions(-Wno-inconsistent-missing-override)
    add_definitions(-Wno-deprecated-declarations)
    add_definitions(-Wno-null-dereference)
    add_definitions(-Wno-deprecated-register)
    add_definitions(-Wno-c++17-extensions)
    # add_definitions(-Wno-defaulted-function-deleted)
endif ()

find_package(yaml-cpp REQUIRED)
set(yaml_libs yaml-cpp)

add_definitions('-g')
add_definitions(${LLVM_DEFINITIONS})

# parent project may already found swig, skip if so.
# the check is added because the find may failed on cmake < 3.20
if (NOT SWIG_FOUND)
    set(SWIG_DIR ${CMAKE_PREFIX_PATH}/share/swig/4.0.1)
    find_package(SWIG REQUIRED)
    include(UseSWIG)
else()
    message(STATUS "SWIG already found: ${SWIG_EXECUTABLE}:${SWIG_VERSION}")
endif()

include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${LLVM_INCLUDE_DIRS}
    ${Boost_INCLUDE_DIRS}
    ${CMAKE_PREFIX_PATH}/include
)

link_directories(
    ${LLVM_LIBRARY_DIRS}
    ${Boost_LIBRARY_DIRS}
    ${CMAKE_PREFIX_PATH}/lib
    ${CMAKE_PREFIX_PATH}/lib64)

find_package(absl REQUIRED)
# modify absl::time_zone INTERFACE_LINK_LIBRARIES
set_target_properties(absl::time_zone PROPERTIES INTERFACE_LINK_LIBRARIES "\$<\$<PLATFORM_ID:Darwin>:-framework CoreFoundation>")

find_package(GTest REQUIRED)

list(
        APPEND
        ABSL_LIBS
        absl::algorithm
        absl::base
        absl::cord
        absl::debugging
        absl::flat_hash_map
        absl::flags
        absl::flags_parse
        absl::memory
        absl::meta
        absl::numeric
        absl::random_random
        absl::strings
        absl::strings_internal
        absl::synchronization
        absl::time
        absl::status
        absl::statusor
)

find_package(ICU COMPONENTS i18n io uc data)
if (NOT ICU_FOUND)
    message(STATUS "ICU package Not Found")
    find_library(ICU_I18N icui18n)
    find_library(ICU_IO icuio)
    find_library(ICU_UC icuuc)
    find_library(ICU_DATA icudata)
    set(ICU_LIBRARIES ${ICU_I18N} ${ICU_IO} ${ICU_UC} ${ICU_DATA})
endif ()
message(STATUS "Found ICU Libraries: ${ICU_LIBRARIES}")

set(ZETASQL_LIBS
        zetasql
        ${ABSL_LIBS}
        re2
        ${ICU_LIBRARIES}
    )

# Find java
if (SQL_JAVASDK_ENABLE)
    find_package(Java COMPONENTS Development REQUIRED)
    message(STATUS "Found Java: ${Java_JAVA_EXECUTABLE} (found version \"${Java_VERSION_STRING}\")")

    find_package(JNI REQUIRED)
    message(STATUS "Found JNI: ${JNI_FOUND}")
endif ()

add_subdirectory(src)
#TODO add flag
if (EXAMPLES_ENABLE)
    add_subdirectory(examples)
endif ()

if (CMAKE_PROJECT_NAME STREQUAL "openmldb")
  # propagate test_list to upper scope
  set(test_list ${test_list} PARENT_SCOPE)
endif()
