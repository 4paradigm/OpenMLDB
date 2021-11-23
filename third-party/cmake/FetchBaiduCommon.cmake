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


set(COMMON_URL https://github.com/4paradigm/common/archive/refs/tags/v1.0.0.tar.gz)

message(STATUS "build baidu common from ${COMMON_URL}")

find_program(MAKE_EXE NAMES gmake nmake make REQUIRED)
if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  set(BOOST_PATCH bash -c "sed -i '' 's/^#include <syscall.h>/#include <pthread.h>/' src/logging.cc"
        COMMAND bash -c "sed -i '' 's/thread_id = syscall(__NR_gettid)/pthread_threadid_np(0, \&thread_id)/' src/logging.cc")
endif()

ExternalProject_Add(
  baiducommon
  DEPENDS boost
  URL ${COMMON_URL}
  URL_HASH SHA256=458d525809a53e491890eaa78318c22b3261fc3a8cc8cfdbbdb0715767f4f434
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/baiducommon
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  BUILD_IN_SOURCE True
  PATCH_COMMAND ${BOOST_PATCH}
  CONFIGURE_COMMAND ""
  BUILD_COMMAND bash -c "${MAKE_EXE} ${MAKEOPTS} INCLUDE_PATH='-Iinclude -I<INSTALL_DIR>/include' PREFIX=<INSTALL_DIR> install"
  INSTALL_COMMAND "")
