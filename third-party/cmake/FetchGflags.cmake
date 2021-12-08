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

set(GFLAGS_URL https://github.com/gflags/gflags/archive/refs/tags/v2.2.0.tar.gz)

message(STATUS "build gflags from ${GFLAGS_URL}")

ExternalProject_Add(
  gflags
  URL ${GFLAGS_URL}
  URL_HASH SHA256=466c36c6508a451734e4f4d76825cf9cd9b8716d2b70ef36479ae40f08271f88
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/gflags
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  CONFIGURE_COMMAND ${CMAKE_COMMAND} -H<SOURCE_DIR> -B <BINARY_DIR>
    -DCMAKE_INSTALL_PREFIX=${DEPS_INSTALL_DIR} -DGFLAGS_NAMESPACE=google -DCMAKE_CXX_FLAGS=-fPIC
  BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR>
  INSTALL_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --target install)
