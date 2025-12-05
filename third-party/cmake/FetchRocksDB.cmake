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

set(SRC_URL https://github.com/facebook/rocksdb/archive/v6.27.3.tar.gz)
message(STATUS "build rocksdb from ${SRC_URL}")

if (${CMAKE_CXX_COMPILER_ID} STREQUAL "AppleClang" AND ${CMAKE_CXX_COMPILER_VERSION} VERSION_GREATER_EQUAL "13.1.6")
  message(STATUS "apply rocksdb patch for apple clang 13.1.6 or later")
  set(PATCH_CMD patch -p 1 -N -i ${PROJECT_SOURCE_DIR}/patches/rocksdb.patch)
endif()

find_program(MAKE_EXE NAMES gmake nmake make REQUIRED)
ExternalProject_Add(
  rocksdb
  URL ${SRC_URL}
  URL_HASH SHA256=ee29901749b9132692b26f0a6c1d693f47d1a9ed8e3771e60556afe80282bf58
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/rocksdb
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  PATCH_COMMAND ${PATCH_CMD}
  CONFIGURE_COMMAND
    ${CMAKE_COMMAND} -H<SOURCE_DIR> -B<BINARY_DIR> -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>
    -DUSE_RTTI=ON -DROCKSDB_BUILD_SHARED=OFF -DWITH_TESTS=OFF -DWITH_TOOLS=OFF
    -DWITH_BENCHMARK_TOOLS=OFF -DWITH_GFLAGS=OFF -DWITH_JNI=OFF -DJNI=OFF -DPORTABLE=ON ${CMAKE_OPTS}
  BUILD_COMMAND ""
  INSTALL_COMMAND
    ${CMAKE_COMMAND} --build <BINARY_DIR> --target install -- ${MAKEOPTS})
