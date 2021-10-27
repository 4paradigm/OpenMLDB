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

include(ExternalProject)

set(DEPS_INSTALL_DIR "${CMAKE_BINARY_DIR}/usr" CACHE PATH "Dependencies install directory.")
set(DEPS_BUILD_DIR "${CMAKE_BINARY_DIR}/build" CACHE PATH "Dependencies build directory.")
set(DEPS_DOWNLOAD_DIR "${DEPS_BUILD_DIR}/downloads" CACHE PATH "Dependencies download directory.")

option(USE_BUNDLED "Build dependencies from source" OFF)
option(USE_BUNDLED_ZETASQL "Build zetasql from source" ${USE_BUNDLED})
option(USE_BUNDLED_HYBRIDSQL_ASSERTS "Build hybridsql asserts from source" OFF)

set(ZETASQL_HOME https://github.com/aceforeverd/zetasql)
set(ZETASQL_VERSION 0.2.1.beta9)
set(ZETASQL_TAG origin/feat/hybridse-zetasql)
set(HYBRIDSQL_ASSERTS_HOME https://github.com/jingchen2222/hybridsql-asserts)
set(HYBRIDSQL_ASSERTS_VERSION 0.4.0)

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
  if (CMAKE_SYSTEM_PROCESSOR MATCHES "(arm64)|(ARM64)|(aarch64)|(AARCH64)")
    set(ARCH_NAME aarch64)
  else()
    set(ARCH_NAME x86_64)
  endif()

  set(ZETASQL_URL "${ZETASQL_HOME}/releases/download/v${ZETASQL_VERSION}/libzetasql-${ZETASQL_VERSION}-linux-gnu-${ARCH_NAME}.tar.gz")
  set(HYBRIDSQL_ASSERTS_URL "${HYBRIDSQL_ASSERTS_HOME}/releases/download/v${HYBRIDSQL_ASSERTS_VERSION}/thirdparty-2021-08-03-linux-gnu-${ARCH_NAME}.tar.gz")
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  set(ZETASQL_URL "${ZETASQL_HOME}/releases/download/v${ZETASQL_VERSION}/libzetasql-${ZETASQL_VERSION}-darwin-x86_64.tar.gz")
  set(HYBRIDSQL_ASSERTS_URL "${HYBRIDSQL_ASSERTS_HOME}/releases/download/v${HYBRIDSQL_ASSERTS_VERSION}/thirdparty-2021-08-03-darwin-x86_64.tar.gz")
endif()

if (NOT USE_BUNDLED_ZETASQL)
  message(STATUS "Download pre-compiled zetasql from ${ZETASQL_URL}")
  # download pre-compiled zetasql from GitHub Release
  ExternalProject_Add(zetasql
    URL ${ZETASQL_URL}
    PREFIX ${DEPS_BUILD_DIR}
    DOWNLOAD_DIR "${DEPS_DOWNLOAD_DIR}/zetasql"
    DOWNLOAD_NO_EXTRACT True
    INSTALL_DIR ${DEPS_INSTALL_DIR}
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND "")
  ExternalProject_Get_Property(zetasql DOWNLOADED_FILE)
  add_custom_target(zetasql-extract ALL tar xzf ${DOWNLOADED_FILE} -C ${DEPS_INSTALL_DIR} --strip-components=1
    COMMENT "Extracting zetasql ${DOWNLOADED_FILE} into ${DEPS_INSTALL_DIR}")
  add_dependencies(zetasql-extract zetasql)
else()
  message(STATUS "Compile zetasql from source: ${ZETASQL_HOME}@${ZETASQL_TAG}")
  ExternalProject_Add(zetasql
    GIT_REPOSITORY ${ZETASQL_HOME}
    GIT_TAG ${ZETASQL_TAG}
    PREFIX ${DEPS_BUILD_DIR}
    INSTALL_DIR ${DEPS_INSTALL_DIR}
    BUILD_IN_SOURCE True
    CONFIGURE_COMMAND ""
    BUILD_COMMAND bash build_zetasql_parser.sh
    INSTALL_COMMAND bash pack_zetasql.sh -i ${DEPS_INSTALL_DIR}
  )
endif()

if (NOT USE_BUNDLED_HYBRIDSQL_ASSERTS)
  message(STATUS "Download pre-compiled hybridsql assert from ${HYBRIDSQL_ASSERTS_URL}")
  ExternalProject_Add(hybridsql-asserts
    URL ${HYBRIDSQL_ASSERTS_URL}
    PREFIX ${DEPS_BUILD_DIR}
    DOWNLOAD_DIR "${DEPS_DOWNLOAD_DIR}/hybridsql-asserts"
    DOWNLOAD_NO_EXTRACT True
    INSTALL_DIR ${DEPS_INSTALL_DIR}
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND "")
  ExternalProject_Get_Property(hybridsql-asserts DOWNLOADED_FILE)
  add_custom_target(hybridsql-asserts-extract ALL tar xzf ${DOWNLOADED_FILE} -C ${DEPS_INSTALL_DIR} --strip-components=1
    COMMENT "Extracting hybridsql asserts ${DOWNLOADED_FILE} into ${DEPS_INSTALL_DIR}")
  add_dependencies(hybridsql-asserts-extract hybridsql-asserts)
else()
  message(FATAL_ERROR "Build hybridsql asserts from source is unsupported currently.")
endif()
