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

if (NOT DEFINED DEPS_PREFIX)
  set(DEPS_PREFIX ${CMAKE_SOURCE_DIR}/../thirdparty)
endif()
set(DEPS_BUILD_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/deps"
    CACHE PATH "Dependencies build directory.")
set(DEPS_DOWNLOAD_DIR
    "${DEPS_BUILD_DIR}/downloads"
    CACHE PATH "Dependencies download directory.")

option(USE_BUNDLED "Build dependencies from source" OFF)
option(USE_BUNDLED_ZETASQL "Build zetasql from source" ${USE_BUNDLED})

set(ZETASQL_HOME https://github.com/aceforeverd/zetasql)
set(ZETASQL_VERSION 0.2.1.beta9)

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(ZETASQL_URL "${ZETASQL_HOME}/releases/download/v${ZETASQL_VERSION}/libzetasql-${ZETASQL_VERSION}-linux-gnu-x86_64.tar.gz")
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  set(ZETASQL_URL "${ZETASQL_HOME}/releases/download/v${ZETASQL_VERSION}/libzetasql-${ZETASQL_VERSION}-darwin-x86_64.tar.gz")
endif()

if (NOT USE_BUNDLED_ZETASQL)
  # download pre-compiled zetasql
  ExternalProject_Add(zetasql
    URL ${ZETASQL_URL}
    PREFIX ${DEPS_BUILD_DIR}
    DOWNLOAD_DIR "${DEPS_DOWNLOAD_DIR}/zetasql"
    DOWNLOAD_NO_EXTRACT True
    INSTALL_DIR ${DEPS_PREFIX}
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND "")
  ExternalProject_Get_Property(zetasql DOWNLOADED_FILE)
  add_custom_target(zetasql_extract ALL tar xzf ${DOWNLOADED_FILE} -C ${DEPS_PREFIX} --strip-components=1
    COMMENT "Extracting zetasql from ${DOWNLOADED_FILE} into ${DEPS_PREFIX}")
  add_dependencies(zetasql_extract zetasql)
else()
  ExternalProject_Add(zetasql
    GIT_REPOSITORY https://github.com/aceforeverd/zetasql.git
    GIT_TAG origin/build/add-version-number
    PREFIX ${DEPS_BUILD_DIR}
    INSTALL_DIR ${DEPS_PREFIX}
    BUILD_IN_SOURCE True
    CONFIGURE_COMMAND ""
    BUILD_COMMAND bash build_zetasql_parser.sh
    INSTALL_COMMAND bash pack_zetasql.sh ${DEPS_PREFIX}
    )
endif()
