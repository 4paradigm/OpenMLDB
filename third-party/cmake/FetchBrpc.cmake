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

set(BRPC_URL https://github.com/4paradigm/incubator-brpc/archive/c94030a1ecacfd0d5f9e2a0e9d05c816eef35ac8.zip)
message(STATUS "build brpc from ${BRPC_URL}")

ExternalProject_Add(
  brpc
  URL ${BRPC_URL}
  URL_HASH SHA256=d981e36ebd54537c5f0d46367adb48886d660f3d7cecc6737817870554a8588b
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/brpc
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  DEPENDS gflags glog protobuf snappy leveldb gperf openssl
  CONFIGURE_COMMAND ${CMAKE_COMMAND} -H<SOURCE_DIR> -B . -DWITH_GLOG=ON -DCMAKE_PREFIX_PATH=${DEPS_INSTALL_DIR} -DCMAKE_INSTALL_PREFIX=${DEPS_INSTALL_DIR} ${CMAKE_OPTS}
  BUILD_COMMAND ${CMAKE_COMMAND} --build . --target brpc-static -- ${MAKEOPTS}
  INSTALL_COMMAND bash -c "cp -rvf output/include/* <INSTALL_DIR>/include/"
    COMMAND cp -v output/lib/libbrpc.a <INSTALL_DIR>/lib)

