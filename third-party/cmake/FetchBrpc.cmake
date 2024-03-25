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

set(BRPC_URL https://github.com/apache/brpc)
set(BRPC_TAG d2b73ec955dd04b06ab55065d9f3b4de1e608bbd)
message(STATUS "build brpc from ${BRPC_URL}@${BRPC_TAG}")

find_package(Git REQUIRED)

ExternalProject_Add(
  brpc
  GIT_REPOSITORY ${BRPC_URL}
  GIT_TAG ${BRPC_TAG}
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/brpc
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  PATCH_COMMAND ${GIT_EXECUTABLE} checkout .
    COMMAND ${GIT_EXECUTABLE} clean -dfx .
    COMMAND ${GIT_EXECUTABLE} apply ${PROJECT_SOURCE_DIR}/patches/brpc-1.6.0.patch
    COMMAND ${GIT_EXECUTABLE} apply ${PROJECT_SOURCE_DIR}/patches/brpc-1.6.0-2235.patch
  DEPENDS gflags glog protobuf snappy leveldb gperf openssl
  # BRPC get problemistic with CMAKE_BUILD_TYPE=Release (-O3), and seems to having its own
  # CPP flags by default, so we do not inherit global CMAKE_OPTS variable
  CONFIGURE_COMMAND
    ${CMAKE_COMMAND} -H<SOURCE_DIR> -B . -DWITH_GLOG=ON
    -DCMAKE_PREFIX_PATH=${DEPS_INSTALL_DIR} -DCMAKE_INSTALL_PREFIX=${DEPS_INSTALL_DIR}
  BUILD_COMMAND ${CMAKE_COMMAND} --build . --target brpc-static -- ${MAKEOPTS}
  INSTALL_COMMAND bash -c "cp -rvf output/include/* <INSTALL_DIR>/include/"
    COMMAND cp -v output/lib/libbrpc.a <INSTALL_DIR>/lib)

