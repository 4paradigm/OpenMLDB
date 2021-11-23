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


set(SQLITE_URL https://github.com/sqlite/sqlite/archive/version-3.32.3.zip)

message(STATUS "build sqlite3 from ${SQLITE_URL}")

find_program(MAKE_EXE NAMES gmake nmake make)

ExternalProject_Add(
  sqlite3
  URL ${SQLITE_URL}
  URL_HASH SHA256=7bf644e17080c59975e158818a35bfc4e49d82f2c264e105bc808d9b0bc8a143
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/sqlite3
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  CONFIGURE_COMMAND <SOURCE_DIR>/configure --prefix=<INSTALL_DIR> --disable-tcl --enable-shared=no
  BUILD_COMMAND ${MAKE_EXE} ${MAKEOPTS}
  INSTALL_COMMAND ${MAKE_EXE} install)
