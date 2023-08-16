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

set(BRPC_URL https://github.com/4paradigm/incubator-brpc/archive/a85d1bde8df3a3e2e59a64ea5a3ee3122f9c6daa.zip)
message(STATUS "build brpc from ${BRPC_URL}")

if (BUILD_SHARED_LIBS)
  set (BRPC_DEPENDS_TARGETS glog)
else()
  set (BRPC_DEPENDS_TARGETS glog protobuf snappy leveldb gperf openssl)
endif()

ExternalProject_Add(
  brpc
  URL ${BRPC_URL}
  URL_HASH SHA256=ea86d39313bed981357d2669daf1a858fcf1ec363465eda2eec60a8504a2c38e
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/brpc
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  DEPENDS ${BRPC_DEPENDS_TARGETS} ${GENERAL_DEPS}
  CONFIGURE_COMMAND ${CMAKE_COMMAND} -H<SOURCE_DIR> -B . -DWITH_GLOG=ON -DCMAKE_PREFIX_PATH=${DEPS_INSTALL_DIR} -DCMAKE_INSTALL_PREFIX=${DEPS_INSTALL_DIR} ${CMAKE_OPTS}
  BUILD_COMMAND ${CMAKE_COMMAND} --build . --target brpc-static brpc-shared -- ${MAKEOPTS}
  INSTALL_COMMAND ${CMAKE_COMMAND} --build . --target install -- ${MAKEOPTS}
)

