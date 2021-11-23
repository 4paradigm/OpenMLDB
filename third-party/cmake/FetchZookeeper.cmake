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

set(ZOOKEEPER_URL https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz)
set(ZOOKEEPER_HASH b14f7a0fece8bd34c7fffa46039e563ac5367607c612517aa7bd37306afbd1cd)
set(ZOOKEEPER_WORK_DIR zookeeper-client/zookeeper-client-c/)

if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  set(ZOOKEEPER_CONF COMMAND bash -c "CC=clang CFLAGS='-O3 -fPIC' ./configure --prefix=<INSTALL_DIR> --enable-shared=no")
else()
  find_program(AUTORECONF NAMES autoreconf REQUIRED)
  set(ZOOKEEPER_CONF COMMAND ${AUTORECONF} -if
    COMMAND bash -c "CFLAGS='-O3 -fPIC -Wno-error=format-overflow=' ./configure --prefix=<INSTALL_DIR> --enable-shared=no")
endif()

message(STATUS "build zookeeper from ${ZOOKEEPER_URL}")

find_program(MAKE_EXE NAMES gmake nmake make)

ExternalProject_Add(
  zookeeper
  URL ${ZOOKEEPER_URL}
  URL_HASH SHA256=${ZOOKEEPER_HASH}
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/zookeeper
  BINARY_DIR ${DEPS_BUILD_DIR}/src/zookeeper/${ZOOKEEPER_WORK_DIR}
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  CONFIGURE_COMMAND ${ZOOKEEPER_CONF}
  BUILD_COMMAND ${MAKE_EXE} ${MAKEOPTS}
  INSTALL_COMMAND ${MAKE_EXE} install)
