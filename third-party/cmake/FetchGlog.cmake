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

set(GLOG_URL https://github.com/google/glog)
set(GLOG_TAG b33e3bad4c46c8a6345525fd822af355e5ef9446) #0.6.0

message(STATUS "build glog from ${GLOG_URL}@${GLOG_TAG}")

find_program(MAKE_EXE NAMES gmake nmake make)
ExternalProject_Add(
  glog
  GIT_REPOSITORY ${GLOG_URL}
  GIT_TAG ${GLOG_TAG}
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/glog
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  DEPENDS gflags ${GENERAL_DEPS}
  CONFIGURE_COMMAND
    ${CMAKE_COMMAND} -H<SOURCE_DIR> -B <BINARY_DIR>
    -DCMAKE_INSTALL_PREFIX=${DEPS_INSTALL_DIR} -DCMAKE_PREFIX_PATH=${DEPS_INSTALL_DIR}
    ${CMAKE_OPTS} -DWITH_GTEST=OFF -DBUILD_SHARED_LIBS=${BUILD_SHARED_LIBS} -DWITH_UNWIND=OFF
  BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR>
  INSTALL_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --target install)
