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

set(LEVELDB_URL https://github.com/google/leveldb/archive/refs/tags/1.23.tar.gz)

message(STATUS "build leveldb from ${LEVELDB_URL}")

find_program(MAKE_EXE NAMES gmake nmake make REQUIRED)
ExternalProject_Add(
  leveldb
  URL ${LEVELDB_URL}
  URL_HASH SHA256=9a37f8a6174f09bd622bc723b55881dc541cd50747cbd08831c2a82d620f6d76
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/leveldb
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  BUILD_IN_SOURCE True
  CONFIGURE_COMMAND ""
  BUILD_COMMAND bash -c "${MAKE_EXE} OPT='-O2 -DNDEBUG -fPIC'"
  INSTALL_COMMAND bash -c "cp -rv include/* <INSTALL_DIR>/include/"
    COMMAND cp -v out-static/libleveldb.a <INSTALL_DIR>/lib/)
