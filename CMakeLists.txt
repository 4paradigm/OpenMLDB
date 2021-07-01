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

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake-modules")

project(hybridse)

set(HYBRIDSE_VERSION_MAJOR 0)
set(HYBRIDSE_VERSION_MINOR 1)
set(HYBRIDSE_VERSION_BUG 4)

option(TESTING_ENABLE "Enable Test" ON)
option(CORE_TESTING_ENABLE "Enable Core Test" ON)
option(EXAMPLES_TESTING_ENABLE "Enable Examples Test" ON)
option(BENCHMARK_ENABLE "Enable Benchmark" OFF)
option(COVERAGE_ENABLE "Enable Coverage" OFF)
option(PYSDK_ENABLE "Enable pysdk" ON)
option(JAVASDK_ENABLE "Enable javasdk" ON)
option(EXAMPLES_ENABLE "Enable examples" OFF)
option(LLVM_EXT_ENABLE "Enable llvm ext sources" OFF)

if (NOT DEFINED CMAKE_PREFIX_PATH)
    set(CMAKE_PREFIX_PATH ${CMAKE_SOURCE_DIR}/thirdparty)
endif()

if (CMAKE_BUILD_TYPE STREQUAL "")
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif ()
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(UNIX AND NOT APPLE AND NOT DEFINED SWIG_DIR)
    # TODO: swig 4.0.1 compiled is not portable, check 'swig -swiglib'
    set(SWIG_DIR ${CMAKE_SOURCE_DIR}/thirdparty/share/swig/4.0.1)
endif()

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

find_package(Threads)


set(BOOST_ROOT "${CMAKE_PREFIX_PATH}")
find_package(Boost COMPONENTS filesystem date_time regex REQUIRED)
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(OS_LIB ${CMAKE_THREAD_LIBS_INIT} rt)
    set(COMMON_LIBS boost_date_time boost_filesystem boost_regex leveldb protobuf glog unwind z snappy ssl crypto dl)
    set(BRPC_LIBS brpc ${COMMON_LIBS})
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
    set(COMMON_LIBS boost_date_time boost_filesystem boost_regex leveldb protobuf glog  z snappy ssl crypto dl)
    set(BRPC_LIBS brpc ${COMMON_LIBS})
endif ()

configure_file(
        "${PROJECT_SOURCE_DIR}/src/version.h.in"
        "${PROJECT_SOURCE_DIR}/src/hybridse_version.h"
)

if (TESTING_ENABLE)
    enable_testing()
endif ()
if (COVERAGE_ENABLE)
    if (CMAKE_COMPILER_IS_GNUCXX)
        include(CodeCoverage)
        APPEND_COVERAGE_COMPILER_FLAGS()
    endif ()
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
find_package(SWIG REQUIRED)
include(UseSWIG)

include_directories(${CMAKE_PREFIX_PATH}/include)
include_directories(${PROJECT_SOURCE_DIR}/include)
link_directories(${CMAKE_PREFIX_PATH}/lib)
link_directories(${CMAKE_PREFIX_PATH}/lib64)


find_package(absl REQUIRED)

find_package(GTest 1.10 EXACT REQUIRED)

set(Protobuf_USE_STATIC_LIBS ON)
find_package(Protobuf 3.6 EXACT REQUIRED)
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
        absl::status)

list(APPEND ICU_LIBS icui18n icuio icuuc icudata)
list(APPEND file_based_test_driver_LIBS
        alternations
        test_case_options
        test_case_outputs
        test_case_mode
        logging
        path
        status
        unified_diff_oss
        ret_check
        )
set(ZETASQL_LIBS
        zetasql
        ${file_based_test_driver_LIBS}
        ${ABSL_LIBS}
        re2
        ${ICU_LIBS}
        date_proto
        timeofday_proto
        latlng_proto
    )
if (PYSDK_ENABLE)
    set(Python_ADDITIONAL_VERSIONS "3.8;3.7;3.6;3.5;2.7" CACHE STRING "Python to use for binding")
    find_package(PythonInterp REQUIRED)
    message(STATUS "Found Python: ${PYTHON_EXECUTABLE} (found version \"${PYTHON_VERSION_STRING}\")")
    # Find Python Library
    # Force PythonLibs to find the same version than the python interpreter (or nothing).
    set(Python_ADDITIONAL_VERSIONS "${PYTHON_VERSION_STRING}")
    find_package(PythonLibs REQUIRED)
    message(STATUS "Found Python Include: ${PYTHON_INCLUDE_DIRS} (found version \"${PYTHONLIBS_VERSION_STRING}\")")
endif ()
# Find java

if (JAVASDK_ENABLE)
    find_package(Java COMPONENTS Development REQUIRED)
    message(STATUS "Found Java: ${Java_JAVA_EXECUTABLE} (found version \"${Java_VERSION_STRING}\")")

    find_package(JNI REQUIRED)
    message(STATUS "Found JNI: ${JNI_FOUND}")
    # Find maven
    find_program(MAVEN_EXECUTABLE mvn)
endif ()
add_subdirectory(src)
#TODO add flag
if (EXAMPLES_ENABLE)
    add_subdirectory(examples)
endif ()
if (COVERAGE_ENABLE)
    if (CMAKE_COMPILER_IS_GNUCXX)
        SETUP_TARGET_FOR_COVERAGE_LCOV(NAME coverage
                EXECUTABLE ctest -j ${n_cores}
                DEPENDENCIES ${test_list})
    endif ()
endif ()

if (CMAKE_INSTALL_PREFIX STREQUAL "")
    set(CMAKE_INSTALL_PREFIX ${CMAKE_BINARY_DIR}/release)
endif ()
install(TARGETS hybridse_core hybridse_sdk hybridse_flags
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})

FILE(GLOB header_files "${CMAKE_CURRENT_SOURCE_DIR}/include/*")
INSTALL(DIRECTORY ${header_files} DESTINATION include)
INSTALL(DIRECTORY "${CMAKE_BINARY_DIR}/src/proto" DESTINATION include
    FILES_MATCHING PATTERN "*.h"
    PATTERN "CMakeFiles" EXCLUDE
    )
