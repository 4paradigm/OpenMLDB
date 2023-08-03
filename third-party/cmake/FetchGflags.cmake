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

set(GFLAGS_URL https://github.com/gflags/gflags)
set(GFLAGS_TAG e171aa2d15ed9eb17054558e0b3a6a413bb01067) # v2.2.2

message(STATUS "build gflags from ${GFLAGS_URL}@${GFLAGS_TAG}")

ExternalProject_Add(
  gflags
  GIT_REPOSITORY ${GFLAGS_URL}
  GIT_TAG ${GFLAGS_TAG}
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/gflags
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  CONFIGURE_COMMAND ${CMAKE_COMMAND} -H<SOURCE_DIR> -B <BINARY_DIR>
    -DCMAKE_INSTALL_PREFIX=${DEPS_INSTALL_DIR} -DGFLAGS_BUILD_STATIC_LIBS=ON
    -DGFLAGS_NAMESPACE=google -DCMAKE_CXX_FLAGS=-fPIC
  BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR>
  INSTALL_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --target install)
