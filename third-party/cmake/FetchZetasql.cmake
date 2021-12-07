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

set(ZETASQL_HOME https://github.com/aceforeverd/zetasql)
set(ZETASQL_VERSION 0.2.4.beta2)
set(ZETASQL_TAG origin/feat/hybridse-zetasql)

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(ZETASQL_URL "${ZETASQL_HOME}/releases/download/v${ZETASQL_VERSION}/libzetasql-${ZETASQL_VERSION}-linux-gnu-x86_64.tar.gz")
  set(ZETASQL_HASH 4b32d4248d4a371bb1355bc9e7ad566e67525cfd43e7afaab4f4d9cc432a99be)
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  set(ZETASQL_URL "${ZETASQL_HOME}/releases/download/v${ZETASQL_VERSION}/libzetasql-${ZETASQL_VERSION}-darwin-x86_64.tar.gz")
  set(ZETASQL_HASH f530b5c393b7653e2180747867852c0ddbcf16a6a18a89e0249e8dd2fa9fdb59)
endif()


if (NOT BUILD_BUNDLED_ZETASQL)
  if (CMAKE_SYSTEM_PROCESSOR MATCHES "(arm64)|(ARM64)|(aarch64)|(AARCH64)")
    message(FATAL_ERROR pre-compiled zetasql for arm64 not available, please build zetasql from source instead by '-DBUILD_BUNDLED_ZETASQL=ON')
  endif()
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
    INSTALL_COMMAND bash -c "tar xzf <DOWNLOADED_FILE> -C ${DEPS_INSTALL_DIR} --strip-components=1")
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
