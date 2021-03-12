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

project(fesql)

set(FESQL_VERSION_MAJOR 0)
set(FESQL_VERSION_MEDIUM 1)
set(FESQL_VERSION_MINOR 0)
set(FESQL_VERSION_BUG 0)

option(TESTING_ENABLE "Enable Test" ON)
option(BENCHMARK_ENABLE "Enable Benchmark" ON)
option(COVERAGE_ENABLE "Enable Coverage" ON)
option(PYSDK_ENABLE "Enable pysdk" ON)
option(JAVASDK_ENABLE "Enable javasdk" ON)
option(EXAMPLES_ENABLE "Enable examples" ON)
option(LLVM_EXT_ENABLE "Enable llvm ext sources" OFF)

if (CMAKE_BUILD_TYPE STREQUAL "")
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif ()
message (STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# llvm dependency
if (NOT DEFINED LLVM_DIR) 
    set(LLVM_DIR "${PROJECT_SOURCE_DIR}/thirdparty/lib/cmake/llvm")
endif()
find_package(LLVM REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")

if (LLVM_EXT_ENABLE)
    llvm_map_components_to_libnames(LLVM_LIBS
        support core orcjit nativecodegen
        mcjit executionengine IntelJITEvents PerfJITEvents object)
else() 
    llvm_map_components_to_libnames(LLVM_LIBS
        support core orcjit nativecodegen)
endif()

find_package(Threads)

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(OS_LIB ${CMAKE_THREAD_LIBS_INIT} rt)
    set(COMMON_LIBS boost_date_time boost_filesystem boost_regex leveldb protobuf glog gflags unwind lzma z snappy ssl crypto dl)
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
    set(COMMON_LIBS boost_date_time boost_filesystem boost_regex leveldb protobuf glog gflags z snappy ssl crypto dl)
    set(BRPC_LIBS brpc ${COMMON_LIBS})
endif ()

set(BOOST_ROOT "${PROJECT_SOURCE_DIR}/thirdparty/")
set(abseil-cpp_DIR "${PROJECT_SOURCE_DIR}/thirdparty/")
configure_file(
        "${PROJECT_SOURCE_DIR}/src/version.h.in"
        "${PROJECT_SOURCE_DIR}/src/fe_version.h"
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
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    add_definitions('-Wall')
    add_definitions('-fexceptions')
    add_definitions(-Wreorder)
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

set(boost_libs boost_graph)
find_package(yaml-cpp REQUIRED)
set(yaml_libs yaml-cpp)
set(ABSL_LIBS absl_time absl_time_zone absl_civil_time absl_base absl_int128)
add_definitions('-g')
add_definitions('-std=c++14')
add_definitions(${LLVM_DEFINITIONS})
find_package(SWIG REQUIRED)
include(UseSWIG)

include_directories(${PROJECT_SOURCE_DIR}/thirdparty/include)
link_directories(${PROJECT_SOURCE_DIR}/thirdparty/lib)
link_directories(${PROJECT_SOURCE_DIR}/thirdparty/lib64)

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
    set(JAVA_HOME ${PROJECT_SOURCE_DIR}/thirdparty/jdk1.8.0_141)
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

set(CMAKE_INSTALL_PREFIX ${CMAKE_BINARY_DIR}/release)

install(TARGETS fesql_core fesql_sdk
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})

