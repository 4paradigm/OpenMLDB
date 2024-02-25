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

if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  message(NOTICE "Darwin has libunwind builtin, skip compile libunwind")
  return()
endif()

set(LIBUNWIND_URL https://github.com/libunwind/libunwind/releases/download/v1.5/libunwind-1.5.0.tar.gz)
set(LIBUNWIND_HASH 90337653d92d4a13de590781371c604f9031cdb50520366aa1e3a91e1efb1017)

message(STATUS "build libunwind from ${LIBUNWIND_URL}")

find_program(MAKE_EXE NAMES gmake nmake make REQUIRED)

ExternalProject_Add(
  libunwind
  URL ${LIBUNWIND_URL}
  URL_HASH SHA256=${LIBUNWIND_HASH}
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/libunwind
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  BUILD_IN_SOURCE True
  PATCH_COMMAND ${LIBUNWIND_PATCH}
  # minidebuginfo requires lzma, skip it
  CONFIGURE_COMMAND bash -c "${CONFIGURE_OPTS} ./configure --prefix=<INSTALL_DIR> --enable-shared=no --enable-minidebuginfo=no"
  BUILD_COMMAND ${MAKE_EXE} ${MAKEOPTS}
  INSTALL_COMMAND ${MAKE_EXE} install)
