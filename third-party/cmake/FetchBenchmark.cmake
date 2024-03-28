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

set(BENCHMARK_URL https://github.com/google/benchmark/archive/v1.5.0.tar.gz)

message(STATUS "build benchmark from ${BENCHMARK_URL}")

set(BENCHMARK_OPTS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE})
if (${CMAKE_CXX_COMPILER_ID} STREQUAL "AppleClang" AND ${CMAKE_CXX_COMPILER_VERSION} VERSION_GREATER_EQUAL "13.1.6")
  set(BENCHMARK_OPTS "${BENCHMARK_OPTS} -DCMAKE_CXX_FLAGS='${CMAKE_CXX_FLAGS} -Wno-unused-but-set-variable'")
else()
  set(BENCHMARK_OPTS "${BENCHMARK_OPTS} -DCMAKE_CXX_FLAGS='${CMAKE_CXX_FLAGS}")
endif()


ExternalProject_Add(
  benchmark
  URL ${BENCHMARK_URL}
  URL_HASH SHA256=3c6a165b6ecc948967a1ead710d4a181d7b0fbcaa183ef7ea84604994966221a
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/benchmark
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  CONFIGURE_COMMAND ${CMAKE_COMMAND} -H<SOURCE_DIR> -B <BINARY_DIR>
    -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR> -DCMAKE_CXX_FLAGS=-fPIC -DBENCHMARK_ENABLE_GTEST_TESTS=OFF
    ${BENCHMARK_OPTS}
  BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> -- ${MAKEOPTS}
  INSTALL_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --target install)
