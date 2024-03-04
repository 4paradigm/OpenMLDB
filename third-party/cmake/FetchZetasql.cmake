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
set(ZETASQL_VERSION 0.3.4-b0)
set(ZETASQL_HASH_DARWIN 0cd74b469396f877751cf7efd39685cc4fc544608aef4a901f3f640c3d8aa947)
set(ZETASQL_HASH_LINUX_UBUNTU f94e97ad0b70b34d876cd3820f64756a164b061f3cb40a089e179f2997995bdc)
set(ZETASQL_HASH_LINUX_CENTOS 8e853c4dd9b6638092357fa250ee29e634a1bc8b85ab8c829fbafac1b43ee8b8)
set(ZETASQL_TAG v${ZETASQL_VERSION})

function(init_zetasql_urls)
  if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    get_linux_lsb_release_information()

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
  init_zetasql_urls()

  if (CMAKE_SYSTEM_PROCESSOR MATCHES "(arm64)|(ARM64)|(aarch64)|(AARCH64)")
    message(FATAL_ERROR "pre-compiled zetasql for arm64 not available, try compile zetasql from source by cmake flag: '-DBUILD_BUNDLED_ZETASQL=ON'")
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
