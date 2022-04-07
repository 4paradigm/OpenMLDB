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


set(COMMON_HOME https://github.com/4paradigm/common)
set(COMMON_TAG 5fd6418a65116e223372c45cc949893467895637)

message(STATUS "build baidu common from ${COMMON_HOME}@${COMMON_TAG}")

find_program(MAKE_EXE NAMES gmake nmake make REQUIRED)
if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  set(COMMON_PATCH git apply ${PROJECT_SOURCE_DIR}/patches/baidu_common_darwin.patch)
endif()

ExternalProject_Add(
  baiducommon
  DEPENDS boost
  GIT_REPOSITORY ${COMMON_HOME}
  GIT_TAG ${COMMON_TAG}
  GIT_SHALLOW TRUE
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/baiducommon
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  BUILD_IN_SOURCE True
  PATCH_COMMAND ${COMMON_PATCH}
  CONFIGURE_COMMAND ""
  BUILD_COMMAND bash -c "${CONFIGURE_OPTS} ${MAKE_EXE} ${MAKEOPTS} INCLUDE_PATH='-Iinclude -I<INSTALL_DIR>/include' PREFIX=<INSTALL_DIR> install"
  INSTALL_COMMAND "")
