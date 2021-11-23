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

set(GPERF_URL https://github.com/gperftools/gperftools/releases/download/gperftools-2.5/gperftools-2.5.tar.gz)

message(STATUS "build gperftools from ${GPERF_URL}")

find_program(MAKE_EXE NAMES gmake nmake make REQUIRED)
ExternalProject_Add(
  gperf
  URL ${GPERF_URL}
  URL_HASH SHA256=6fa2748f1acdf44d750253e160cf6e2e72571329b42e563b455bde09e9e85173
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/gperf
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  BUILD_IN_SOURCE True
  CONFIGURE_COMMAND
    ./configure --enable-cpu-profiler --enable-heap-checker --enable-heap-profiler --prefix=<INSTALL_DIR> --enable-shared=no
  BUILD_COMMAND ${MAKE_EXE} ${MAKEOPTS}
  INSTALL_COMMAND ${MAKE_EXE} install)
