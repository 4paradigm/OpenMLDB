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
set(ZETASQL_VERSION 0.2.7)
set(ZETASQL_TAG 4af08ff0bd8d8b46c420b550a01a6542b5718804) # the commit hash for v0.2.7

function(init_zetasql_urls)
  if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    get_linux_lsb_release_information()

    if(CMAKE_SYSTEM_PROCESSOR MATCHES "(arm64)|(ARM64)|(aarch64)|(AARCH64)")
      # there is experimental build for centos7 on aarch64, for others zetasql need compile from source
      if (LSB_RELEASE_ID_SHORT STREQUAL "centos")
        set(ZETASQL_URL "${ZETASQL_HOME}/releases/download/v${ZETASQL_VERSION}/libzetasql-${ZETASQL_VERSION}-linux-gnu-aarch64-centos.tar.gz" PARENT_SCOPE)
        set(ZETASQL_HASH d504ade9973f28507154aa88bf0565939fbf14b4114040f4133c217cdebc93ed PARENT_SCOPE)
      else()
        message(FATAL_ERROR "no pre-compiled zetasql on arm based system except centos7, please compile zetasql with '-DBUILD_BUNDLED_ZETASQL=ON'")
      endif()
    endif()

    if (LSB_RELEASE_ID_SHORT STREQUAL "centos")
      set(ZETASQL_URL "${ZETASQL_HOME}/releases/download/v${ZETASQL_VERSION}/libzetasql-${ZETASQL_VERSION}-linux-gnu-x86_64-centos.tar.gz" PARENT_SCOPE)
      set(ZETASQL_HASH a22f0042924e0abe6ac028c870324b03e2cc2da95b363b77673246515c57773d PARENT_SCOPE)
    elseif(LSB_RELEASE_ID_SHORT STREQUAL "ubuntu")
      set(ZETASQL_URL "${ZETASQL_HOME}/releases/download/v${ZETASQL_VERSION}/libzetasql-${ZETASQL_VERSION}-linux-gnu-x86_64-ubuntu.tar.gz" PARENT_SCOPE)
      set(ZETASQL_HASH eeeb81cc419212d56f99a844b5043f2d61126ef6cc463812f091953b05847bba PARENT_SCOPE)
    else()
      message(FATAL_ERROR "no pre-compiled zetasql for ${LSB_RELEASE_ID_SHORT}, try compile zetasql from source with '-DBUILD_BUNDLED_ZETASQL=ON'")
    endif()
  elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(ZETASQL_URL "${ZETASQL_HOME}/releases/download/v${ZETASQL_VERSION}/libzetasql-${ZETASQL_VERSION}-darwin-x86_64.tar.gz" PARENT_SCOPE)
    set(ZETASQL_HASH a4f4a90f4ac9f3f655f5771e98fcaef8bce57fd05ff2a3e6a1bc9ef8ba733ac9 PARENT_SCOPE)
  endif()
endfunction()


if (NOT BUILD_BUNDLED_ZETASQL)
  init_zetasql_urls()

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
