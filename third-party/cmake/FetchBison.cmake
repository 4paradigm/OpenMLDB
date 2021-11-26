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

set(BISON_URL https://ftp.gnu.org/gnu/bison/bison-3.4.tar.gz)

message(STATUS "build bison from ${BISON_URL}")

find_program(MAKE_EXE NAMES gmake nmake make REQUIRED)

ExternalProject_Add(
  bison
  URL ${BISON_URL}
  URL_HASH SHA256=ee1cc06f5e3d8615a5209cefaa2acd3da59b286d4d923cb6db5e6dbfae7a6c11
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/bison
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  BUILD_IN_SOURCE True
  CONFIGURE_COMMAND bash -c "${CONFIGURE_OPTS} ./configure --prefix=<INSTALL_DIR> --enable-relocatable"
  BUILD_COMMAND ${MAKE_EXE} ${MAKEOPTS}
  INSTALL_COMMAND ${MAKE_EXE} install)

