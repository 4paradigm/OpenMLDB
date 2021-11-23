# Copyright 2021 4paradigm
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

find_program(MAKE_EXE NAMES gmake nmake make REQUIRED)
ExternalProject_Add(
  protobuf
  URL https://github.com/protocolbuffers/protobuf/archive/v3.6.1.3.tar.gz
  URL_HASH SHA256=73fdad358857e120fd0fa19e071a96e15c0f23bb25f85d3f7009abfd4f264a2a
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/protobuf
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  BUILD_IN_SOURCE True
  CONFIGURE_COMMAND ./autogen.sh
    COMMAND bash -c "./configure --disable-shared --with-pic --prefix=<INSTALL_DIR> CPPFLAGS=-I<INSTALL_DIR>/include LDFLAGS=-L<INSTALL_DIR>/lib CXXFLAGS=-std=c++11"
  BUILD_COMMAND ${MAKE_EXE} ${MAKEOPTS}
  INSTALL_COMMAND ${MAKE_EXE} install)
