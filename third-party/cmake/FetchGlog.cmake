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

set(GLOG_URL https://github.com/google/glog/archive/refs/tags/v0.4.0.tar.gz)

message(STATUS "build glog from ${GLOG_URL}")

find_program(MAKE_EXE NAMES gmake nmake make)
ExternalProject_Add(
  glog
  URL ${GLOG_URL}
  URL_HASH SHA256=f28359aeba12f30d73d9e4711ef356dc842886968112162bc73002645139c39c
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/glog
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  DEPENDS gflags
  BUILD_IN_SOURCE TRUE
  CONFIGURE_COMMAND
    ./autogen.sh
    COMMAND CXXFLAGS=-fPIC ./configure --prefix=<INSTALL_DIR> --enable-shared=no --with-gflags=<INSTALL_DIR>
  BUILD_COMMAND ${MAKE_EXE}
  INSTALL_COMMAND ${MAKE_EXE} install)
