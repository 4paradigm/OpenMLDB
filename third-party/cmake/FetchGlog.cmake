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

set(GLOG_URL https://github.com/google/glog/archive/refs/tags/v0.6.0.tar.gz)

message(STATUS "build glog from ${GLOG_URL}")

find_program(MAKE_EXE NAMES gmake nmake make)
ExternalProject_Add(
  glog
  URL ${GLOG_URL}
  URL_HASH SHA256=8a83bf982f37bb70825df71a9709fa90ea9f4447fb3c099e1d720a439d88bad6
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/glog
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  DEPENDS gflags
  BUILD_IN_SOURCE TRUE
  CONFIGURE_COMMAND ${CMAKE_COMMAND} -H<SOURCE_DIR> -B <BINARY_DIR> -DWITH_GLOG=ON -DCMAKE_CXX_FLAGS=-fPIC
    -DBUILD_SHARED_LIBS=OFF -DCMAKE_PREFIX_PATH=<INSTALL_DIR> -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>
  BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> -- ${MAKEOPTS}
  INSTALL_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --target install)
