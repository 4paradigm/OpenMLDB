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

set(ZETASQL_HOME https://github.com/4paradigm/zetasql)
set(ZETASQL_VERSION 0.3.5)
set(ZETASQL_HASH_DARWIN bb92d31d90fd6b5ebe84d1c7ebb46e49b8fb2034adca765a9195ed768ac916f3)
set(ZETASQL_HASH_LINUX_UBUNTU 251cb1a37f60ad384936e6e3d7637a07ea72e7d2537d96ef66e7abb0cd9957ba)
set(ZETASQL_HASH_LINUX_CENTOS 6aeba1a121513ba9c0bcfc486943db51e7da62fd1ababf109501a302ddfdfd7d)
set(ZETASQL_HASH_LINUX_AARCH64 e20e03938ec108b1c08203c238841ae781498dd66c5d791f4ecc326440287cdb)
set(ZETASQL_TAG v${ZETASQL_VERSION})

function(init_zetasql_urls)
  if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    get_linux_lsb_release_information()

    if (CMAKE_SYSTEM_PROCESSOR MATCHES "(arm64)|(ARM64)|(aarch64)|(AARCH64)")
      set(ZETASQL_URL "${ZETASQL_HOME}/releases/download/v${ZETASQL_VERSION}/libzetasql-${ZETASQL_VERSION}-linux-gnu-aarch64.tar.gz" PARENT_SCOPE)
      set(ZETASQL_HASH ${ZETASQL_HASH_LINUX_AARCH64} PARENT_SCOPE)
    endif()

    if (LSB_RELEASE_ID_SHORT STREQUAL "centos")
      set(ZETASQL_URL "${ZETASQL_HOME}/releases/download/v${ZETASQL_VERSION}/libzetasql-${ZETASQL_VERSION}-linux-gnu-x86_64-centos.tar.gz" PARENT_SCOPE)
      set(ZETASQL_HASH ${ZETASQL_HASH_LINUX_CENTOS} PARENT_SCOPE)
    elseif(LSB_RELEASE_ID_SHORT STREQUAL "ubuntu")
      set(ZETASQL_URL "${ZETASQL_HOME}/releases/download/v${ZETASQL_VERSION}/libzetasql-${ZETASQL_VERSION}-linux-gnu-x86_64-ubuntu.tar.gz" PARENT_SCOPE)
      set(ZETASQL_HASH ${ZETASQL_HASH_LINUX_UBUNTU} PARENT_SCOPE)
    else()
      message(FATAL_ERROR "no pre-compiled zetasql for ${LSB_RELEASE_ID_SHORT}, try compile zetasql from source with cmake flag: '-DBUILD_BUNDLED_ZETASQL=ON'")
    endif()
  elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(ZETASQL_URL "${ZETASQL_HOME}/releases/download/v${ZETASQL_VERSION}/libzetasql-${ZETASQL_VERSION}-darwin-x86_64.tar.gz" PARENT_SCOPE)
    set(ZETASQL_HASH ${ZETASQL_HASH_DARWIN} PARENT_SCOPE)
  endif()
endfunction()


if (NOT BUILD_BUNDLED_ZETASQL)
  if (DEFINED ENV{ZETASQL_VERSION} AND "$ENV{ZETASQL_VERSION}" STREQUAL "${ZETASQL_VERSION}")
    message(STATUS "ZETASQL_VERSION from env matches ZETASQL_VERSION, skipping download.")
  else()
    init_zetasql_urls()
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
  endif()
else()
  find_program(BAZEL_EXE NAMES bazel REQUIRED DOC "Compile zetasql require bazel or bazelisk")
  find_program(PYTHON_EXE NAMES python REQUIRED DOC "Compile zetasql require python")
  message(STATUS "Compile zetasql from source: ${ZETASQL_HOME}@${ZETASQL_TAG}")
  ExternalProject_Add(zetasql
    GIT_REPOSITORY ${ZETASQL_HOME}
    GIT_TAG ${ZETASQL_TAG}
    GIT_SHALLOW TRUE
    PREFIX ${DEPS_BUILD_DIR}
    INSTALL_DIR ${DEPS_INSTALL_DIR}
    BUILD_IN_SOURCE True
    CONFIGURE_COMMAND ""
    BUILD_COMMAND bash build_zetasql_parser.sh
    INSTALL_COMMAND bash pack_zetasql.sh -i ${DEPS_INSTALL_DIR}
  )
endif()
