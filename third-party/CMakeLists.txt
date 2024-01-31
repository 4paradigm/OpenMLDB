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

# convention: put all options here
# By default, thirdparty dependencies was fetched/build into the same directory with OpenMLDB
option(BUILD_BUNDLED "Build dependencies from source" OFF)
option(BUILD_BUNDLED_ZETASQL "Build zetasql from source" ${BUILD_BUNDLED})
option(WITH_ZETASQL "Download and build zetasql" ON)

option(BUILD_BUNDLED_ABSL "Build abseil-cpp from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_PROTOBUF "Build protobuf from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_GTEST "Build google test from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_GFLAGS "Build gflags from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_GLOG "build glog from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_GPERF "Build gerftools from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_BENCHMARK "Build benchmark from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_ZLIB "Build zlib from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_SWIG "Build swig from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_YAMLCPP "Build yaml-cpp from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_SNAPPY "Build snappy from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_LEVELDB "Build leveldb from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_LIBUNWIND "Build libunwind from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_SQLITE3 "Build sqlite3 from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_OPENSSL "Build openssl from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_BRPC "Build brpc from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_ZOOKEEPER "Build zookeeper from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_BOOST "Build boost from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_LLVM "Build llvm from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_COMMON "Build baidu common from source" ${BUILD_BUNDLED})
option(BUILD_BUNDLED_ROCKSDB "Build rocksdb from source" ${BUILD_BUNDLED})

# '-O3 -fPIC' reserved for thirdparty compilation
set(CMAKE_CXX_FLAGS "-O3 -fPIC ${CMAKE_CXX_FLAGS}")
set(CMAKE_C_FLAGS "-O3 -fPIC ${CMAKE_C_FLAGS}")
# Since ExternalProject don't inherit cmake settings, need pass specific toolchain configuration through command line
# this is the default environment variables passed, either though './configure'
set(CONFIGURE_OPTS "CFLAGS='${CMAKE_C_FLAGS}' CXXFLAGS='${CMAKE_CXX_FLAGS}'")
set(CMAKE_OPTS -DCMAKE_C_FLAGS='${CMAKE_C_FLAGS}' -DCMAKE_CXX_FLAGS='${CMAKE_CXX_FLAGS}')
# default options psssed to unix make, used when a dependency compilation:
# 1. use make directly
# 2. cmake project generate Unix Makefile project
set(MAKEOPTS "$ENV{MAKEOPTS}" CACHE STRING "Extra options to make")

message(STATUS "Install bundled dependencies into ${DEPS_INSTALL_DIR}")

set(HYBRIDSQL_ASSERTS_HOME https://github.com/4paradigm/hybridsql-asserts)
set(HYBRIDSQL_ASSERTS_VERSION 0.6.1)

function(get_linux_lsb_release_information)
  execute_process(COMMAND bash ${CMAKE_SOURCE_DIR}/get-lsb-release.sh
    OUTPUT_VARIABLE LSB_RELEASE_ID_SHORT OUTPUT_STRIP_TRAILING_WHITESPACE)

  set(LSB_RELEASE_ID_SHORT "${LSB_RELEASE_ID_SHORT}" PARENT_SCOPE)
endfunction()

# setup download urls for hybridsql pre-compiled thirdparty
# it create variables based on different operation system
function(init_hybridsql_thirdparty_urls)
  if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    get_linux_lsb_release_information()

    if(CMAKE_SYSTEM_PROCESSOR MATCHES "(arm64)|(ARM64)|(aarch64)|(AARCH64)")
      # NOTE(#1138): pre-compiled thirdparty is out-of-date
      set(HYBRIDSQL_ASSERTS_URL "https://github.com/4paradigm/hybridsql-asserts/releases/download/v0.4.0/thirdparty-2021-08-03-linux-gnu-aarch64.tar.gz" PARENT_SCOPE)
      set(HYBRIDSQL_ASSERTS_HASH 128e2147903c6b4f5649284326c60ba048ff369f2925f9957f2973a515c65d75 PARENT_SCOPE)
    else()
      if (LSB_RELEASE_ID_SHORT STREQUAL "centos")
        set(HYBRIDSQL_ASSERTS_URL "${HYBRIDSQL_ASSERTS_HOME}/releases/download/v${HYBRIDSQL_ASSERTS_VERSION}/thirdparty-${HYBRIDSQL_ASSERTS_VERSION}-linux-gnu-x86_64-centos.tar.gz" PARENT_SCOPE)
        set(HYBRIDSQL_ASSERTS_HASH 745d0f29cdc0e6073cd83f51e4fdc045622e9027e1cd29f6ef42ca67ac4d726f PARENT_SCOPE)
      elseif(LSB_RELEASE_ID_SHORT STREQUAL "ubuntu")
        set(HYBRIDSQL_ASSERTS_URL "${HYBRIDSQL_ASSERTS_HOME}/releases/download/v${HYBRIDSQL_ASSERTS_VERSION}/thirdparty-${HYBRIDSQL_ASSERTS_VERSION}-linux-gnu-x86_64-ubuntu.tar.gz" PARENT_SCOPE)
        set(HYBRIDSQL_ASSERTS_HASH 4ee22e1d1b976273c0cb2db54646bc047b1d83f3643697924cff94e9ebd06212 PARENT_SCOPE)
      else()
        message(FATAL_ERROR "no pre-compiled thirdparty for your operation system, try compile thirdparty from source with '-DBUILD_BUNDLED=ON'")
      endif()
    endif()
  elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(HYBRIDSQL_ASSERTS_URL "${HYBRIDSQL_ASSERTS_HOME}/releases/download/v${HYBRIDSQL_ASSERTS_VERSION}/thirdparty-${HYBRIDSQL_ASSERTS_VERSION}-darwin-i386.tar.gz" PARENT_SCOPE)
    set(HYBRIDSQL_ASSERTS_HASH bbaef85b441305dc764b403a3f1ef82e11776ceae09b0e3411ab50a5f5adca33 PARENT_SCOPE)
  endif()
endfunction()

if(WITH_ZETASQL)
  include(FetchZetasql)
endif()

# Fetch hybridsql asserts
if(NOT BUILD_BUNDLED)
  init_hybridsql_thirdparty_urls()

  message(STATUS "Download pre-compiled hybridsql assert from ${HYBRIDSQL_ASSERTS_URL}")
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "(arm64)|(ARM64)|(aarch64)|(AARCH64)")
    message(WARNING pre-compiled hybridsql aseerts for arm64 may out-of-date, consider build from source by '-DBUILD_BUNDLED=ON')
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
endif()

if (BUILD_BUNDLED_GTEST)
  include(FetchGoogleTest)
endif()

# It's better that glog depends on gflags
if (BUILD_BUNDLED_GFLAGS)
  include(FetchGflags)
endif()

if (BUILD_BUNDLED_GLOG)
  include(FetchGlog)
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

if (BUILD_BUNDLED_ZLIB)
  include(FetchZlib)
endif()

if (BUILD_BUNDLED_PROTOBUF)
  include(FetchProtobuf)
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

# zookeeper source always needed
include(FetchZookeeper)

if (BUILD_BUNDLED_BOOST)
  include(FetchBoost)
endif()

if (BUILD_BUNDLED_LLVM)
  include(FetchLLVM)
endif()

if (BUILD_BUNDLED_COMMON)
  include(FetchBaiduCommon)
endif()

if (BUILD_BUNDLED_ROCKSDB)
  include(FetchRocksDB)
endif()
