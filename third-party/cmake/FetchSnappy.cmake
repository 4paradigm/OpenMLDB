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


set(SNAPPY_URL https://src.fedoraproject.org/lookaside/pkgs/snappy/snappy-1.1.1.tar.gz/8887e3b7253b22a31f5486bca3cbc1c2/snappy-1.1.1.tar.gz)

message(STATUS "build snappy from ${SNAPPY_URL}")

find_program(MAKE_EXE NAMES gmake nmake make REQUIRED)

ExternalProject_Add(
  snappy
  URL ${SNAPPY_URL}
  URL_HASH SHA256=d79f04a41b0b513a014042a4365ec999a1ad5575e10d5e5578720010cb62fab3
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/snappy
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  BUILD_IN_SOURCE True
  CONFIGURE_COMMAND ./configure --prefix=<INSTALL_DIR> --disable-shared --with-pic
  BUILD_COMMAND ${MAKE_EXE} ${MAKEOPTS}
  INSTALL_COMMAND ${MAKE_EXE} install)
