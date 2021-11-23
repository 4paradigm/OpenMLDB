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

cmake_minimum_required(VERSION 3.10)
project(hybridsql-deps)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

include(ExternalProject)

set(DEPS_BUILD_DIR "${CMAKE_BINARY_DIR}/build" CACHE PATH "Dependencies build directory.")

set(DEPS_DOWNLOAD_DIR "${DEPS_BUILD_DIR}/downloads" CACHE PATH "Dependencies download directory.")
set(DEPS_INSTALL_DIR "${CMAKE_BINARY_DIR}/usr" CACHE PATH "Dependencies install directory.")
set(SRC_INSTALL_DIR "${CMAKE_BINARY_DIR}/src" CACHE PATH "Source code install directory.")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -03 -fPIC")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -03 -fPIC")

# convention: put all options here
# By default, thirdparty dependencies was fetched/build into the same directory with OpenMLDB
option(BUILD_BUNDLED "Build dependencies from source" OFF)
option(BUILD_BUNDLED_ZETASQL "Build zetasql from source" ${BUILD_BUNDLED})

# implementation limit, two steps to install hybridsql-asserts
# 1. if 'BUILD_BUNDLED_HYBRIDSQL_ASSERTS' is OFF, hybridsql asserts will always download
# 2. if you want to compile anything from source, e.g by setting BUILD_BUNDLED_GTEST=ON, gtest will compile
#    and replace those in hybridsql asserts
option(BUILD_BUNDLED_HYBRIDSQL_ASSERTS "Build hybridsql asserts from source" OFF)
option(BUILD_BUNDLED_ABSL "Build abseil-cpp from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_PROTOBUF "Build protobuf from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_GTEST "Build google test from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_GLOG "build glog from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_GFLAGS "Build gflags from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_GPERF "Build gerftools from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_BENCHMARK "Build benchmark from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_ZLIB "Build zlib from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_BISON "Build bison from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_SWIG "Build swig from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_YAMLCPP "Build yaml-cpp from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_SNAPPY "Build snappy from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_LEVELDB "Build leveldb from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_LIBUNWIND "Build libunwind from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_SQLITE3 "Build sqlite3 from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_OPENSSL "Build openssl from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_BRPC "Build brpc from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_ZOOKEEPER "Build zookeeper from source" ${BUILD_BUNDLED})

option(USE_BUNDLED_SRC "Use bundled hybridsql asserts source" ON)

set(MAKEOPTS "$ENV{MAKEOPTS}" CACHE STRING "Extra options to make")

message(STATUS "Install bundled dependencies into ${DEPS_INSTALL_DIR}")

set(HYBRIDSQL_ASSERTS_HOME https://github.com/4paradigm/hybridsql-asserts)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "(arm64)|(ARM64)|(aarch64)|(AARCH64)")
    set(HYBRIDSQL_ASSERTS_URL "${HYBRIDSQL_ASSERTS_HOME}/releases/download/v0.4.0/thirdparty-2021-08-03-linux-gnu-aarch64.tar.gz")
    set(HYBRIDSQL_ASSERTS_HASH 128e2147903c6b4f5649284326c60ba048ff369f2925f9957f2973a515c65d75)
  else()
    set(HYBRIDSQL_ASSERTS_URL "${HYBRIDSQL_ASSERTS_HOME}/releases/download/v0.4.0/thirdparty-2021-08-03-linux-gnu-x86_64.tar.gz")
    set(HYBRIDSQL_ASSERTS_HASH 6f62e3906b2409fc190d09569857d259b2a1cbd363bdd550dfc5e204805d7719)
  endif()
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  set(HYBRIDSQL_ASSERTS_URL "${HYBRIDSQL_ASSERTS_HOME}/releases/download/v0.4.0/thirdparty-2021-08-03-darwin-x86_64.tar.gz")
  set(HYBRIDSQL_ASSERTS_HASH 505cf5068d8d9bd2eed5496d5eda7f83f18f3f2053b064a446876998fad0dc04)
endif()

set(HYBRIDSQL_ASSERTS_SRC_URL "${HYBRIDSQL_ASSERTS_HOME}/releases/download/v0.4.0/thirdsrc-2021-08-03.tar.gz")
set(HYBRIDSQL_ASSERTS_SRC_HASH 6bd44f5d870d6a4e52ed4c8748cc4a53ceff8744609db36d0a722d3d88c51a00)

# Fetch Zetasql
include(FetchZetasql)

# Fetch hybridsql asserts
if(NOT BUILD_BUNDLED_HYBRIDSQL_ASSERTS)
  message(STATUS "Download pre-compiled hybridsql assert from ${HYBRIDSQL_ASSERTS_URL}")
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "(arm64)|(ARM64)|(aarch64)|(AARCH64)")
    message(WARNING pre-compiled hybridsql aseerts for arm64 may out-of-date, consider build from source by '-DBUILD_BUNDLED_HYBRIDSQL_ASSERTS=ON')
  endif()
  ExternalProject_Add(
    hybridsql-asserts
    URL ${HYBRIDSQL_ASSERTS_URL}
    URL_HASH SHA256=${HYBRIDSQL_ASSERTS_HASH}
    PREFIX ${DEPS_BUILD_DIR}
    DOWNLOAD_DIR "${DEPS_DOWNLOAD_DIR}/hybridsql-asserts"
    DOWNLOAD_NO_EXTRACT True
    INSTALL_DIR ${DEPS_INSTALL_DIR}
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND bash -c "tar xzf <DOWNLOADED_FILE> -C ${DEPS_INSTALL_DIR} --strip-components=1")
else()
  message(
    FATAL_ERROR "Build hybridsql asserts from source is unsupported currently.")
endif()

if (BUILD_BUNDLED_GTEST)
  include(FetchGoogleTest)
endif()

if (BUILD_BUNDLED_GLOG)
  include(FetchGlog)
endif()

if (BUILD_BUNDLED_GFLAGS)
  include(FetchGflags)
endif()

if (BUILD_BUNDLED_GPERF)
  include(FetchGperf)
endif()

if (BUILD_BUNDLED_BENCHMARK)
  include(FetchBenchmark)
endif()

if (BUILD_BUNDLED_ABSL)
  include(FetchAbsl)
endif()

if (BUILD_BUNDLED_PROTOBUF)
  include(FetchProtobuf)
endif()

if (BUILD_BUNDLED_ZLIB)
  include(FetchZlib)
endif()

if (BUILD_BUNDLED_BISON)
  include(FetchBison)
endif()

if (BUILD_BUNDLED_SWIG)
  include(FetchSwig)
endif()

if (BUILD_BUNDLED_YAMLCPP)
  include(FetchYamlcpp)
endif()

if (BUILD_BUNDLED_SNAPPY)
  include(FetchSnappy)
endif()

if (BUILD_BUNDLED_LEVELDB)
  include(FetchLeveldb)
endif()

if (BUILD_BUNDLED_LIBUNWIND)
  include(FetchLibunwind)
endif()

if (BUILD_BUNDLED_SQLITE3)
  include(FetchSqlite3)
endif()

if (BUILD_BUNDLED_OPENSSL)
  include(FetchOpenssl)
endif()

if (BUILD_BUNDLED_BRPC)
  include(FetchBrpc)
endif()

if (BUILD_BUNDLED_ZOOKEEPER)
  include(FetchZookeeper)
endif()

if (USE_BUNDLED_SRC)
  message(STATUS "Install source code into ${SRC_INSTALL_DIR}")
  message(STATUS "Download hybridsql source code from ${HYBRIDSQL_ASSERTS_SRC_URL}")
  ExternalProject_Add(
    hybridsql-asserts-src
    URL ${HYBRIDSQL_ASSERTS_SRC_URL}
    URL_HASH SHA256=${HYBRIDSQL_ASSERTS_SRC_HASH}
    PREFIX ${DEPS_BUILD_DIR}
    DOWNLOAD_DIR "${DEPS_DOWNLOAD_DIR}/hybridsql-asserts-src"
    DOWNLOAD_NO_EXTRACT True
    INSTALL_DIR ${SRC_INSTALL_DIR}
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND bash -c "tar xzf <DOWNLOADED_FILE> -C ${SRC_INSTALL_DIR} --strip-components=1")
else()
  message(FATAL_ERROR "'USE_BUNDLED_SRC=OFF' is currently unsupported")
endif()
