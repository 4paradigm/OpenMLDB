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

set(ZETASQL_HOME https://github.com/jingchen2222/zetasql)
set(ZETASQL_VERSION 0.2.1)
set(ZETASQL_TAG origin/feat/hybridse-zetasql)

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
  if (CMAKE_SYSTEM_PROCESSOR MATCHES "(arm64)|(ARM64)|(aarch64)|(AARCH64)")
    # aarch64 support is experimental
    set(ZETASQL_URL "https://github.com/aceforeverd/zetasql/releases/download/v0.2.1.beta9/libzetasql-0.2.1.beta9-linux-gnu-aarch64.tar.gz")
    set(ZETASQL_HASH bba03f6d15504305412d217926092fb4c52051ea3960e1aa82f7445f27f9fb44)
  else()
    set(ZETASQL_URL "${ZETASQL_HOME}/releases/download/v${ZETASQL_VERSION}/libzetasql-${ZETASQL_VERSION}-linux-gnu-x86_64.tar.gz")
    set(ZETASQL_HASH 65f56f424ffa19457dcc6080fd3e7ae310db28ca53f8aaef4221ab6cf9b3a1f1)
  endif()
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  set(ZETASQL_URL "${ZETASQL_HOME}/releases/download/v${ZETASQL_VERSION}/libzetasql-${ZETASQL_VERSION}-darwin-x86_64.tar.gz")
  set(ZETASQL_HASH c8eca82f5eb0622bb5c6f507913b785961d10d2dc59d9122530de903d772ff87)
endif()


if (NOT BUILD_BUNDLED_ZETASQL)
  message(STATUS "Download pre-compiled zetasql from ${ZETASQL_URL}")
  # download pre-compiled zetasql from GitHub Release
  ExternalProject_Add(zetasql
    URL ${ZETASQL_URL}
    URL_HASH SHA256=${ZETASQL_HASH}
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
  find_program(bazel REQUIRED DOC "Building zetasql require bazel")
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
