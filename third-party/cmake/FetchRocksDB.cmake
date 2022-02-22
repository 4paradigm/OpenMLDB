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

set(SRC_URL https://github.com/facebook/rocksdb/archive/v6.27.3.tar.gz)
message(STATUS "build rocksdb from ${SRC_URL}")

find_program(MAKE_EXE NAMES gmake nmake make REQUIRED)
ExternalProject_Add(
  rocksdb
  URL ${SRC_URL}
  URL_HASH SHA256=ee29901749b9132692b26f0a6c1d693f47d1a9ed8e3771e60556afe80282bf58
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/rocksdb
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  BUILD_IN_SOURCE True
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ${MAKE_EXE} static_lib ${MAKEOPTS}
  INSTALL_COMMAND bash -c "cp -rvf ./include/* <INSTALL_DIR>/include/"
    COMMAND mkdir -p <INSTALL_DIR>/lib > /dev/null 2>&1
    COMMAND cp -v librocksdb.a <INSTALL_DIR>/lib/)
