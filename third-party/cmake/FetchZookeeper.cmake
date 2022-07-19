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

option(BUILD_ZOOKEEPER_PATCH "apply workaround patch to zookeeper, which may useful for gcc >= 9" ON)

if (BUILD_BUNDLED_ZOOKEEPER)
  if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(ZOOKEEPER_CONF COMMAND bash -c "CC=clang CFLAGS='-O3 -fPIC' ./configure --prefix=<INSTALL_DIR> --enable-shared=no")
  else()
    find_program(AUTORECONF NAMES autoreconf REQUIRED)
    find_program(PKGCONF NAME pkg-config REQUIRED)
    include(Findcppunit)
    if (NOT CPPUNIT_FOUND)
      message(FATAL_ERROR "zookeeper require cppunit")
    endif()

    if (BUILD_ZOOKEEPER_PATCH)
      set(ZOOKEEPER_PATCH patch -p 1 -N -i ${PROJECT_SOURCE_DIR}/patches/zookeeper.patch)
      set(ZOOKEEPER_CFLAGS "-O3 -fPIC -Wno-error=format-overflow= -Wno-maybe-uninitialized -Wno-stringop-truncation")
    else()
      set(ZOOKEEPER_CFLAGS "-O3 -fPIC -Wno-error=format-overflow=")
    endif()

    set(ZOOKEEPER_CONF COMMAND ${AUTORECONF} -if
      COMMAND bash -c "CFLAGS='${ZOOKEEPER_CFLAGS}' ./configure --prefix=<INSTALL_DIR> --enable-shared=no")
  endif()

  message(STATUS "build zookeeper from ${ZOOKEEPER_URL}")
  find_program(MAKE_EXE NAMES gmake nmake make REQUIRED)

  ExternalProject_Add(
    zookeeper
    URL ${ZOOKEEPER_URL}
    URL_HASH SHA256=${ZOOKEEPER_HASH}
    PREFIX ${DEPS_BUILD_DIR}
    DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/zookeeper
    BINARY_DIR ${DEPS_BUILD_DIR}/src/zookeeper/${ZOOKEEPER_WORK_DIR}
    INSTALL_DIR ${DEPS_INSTALL_DIR}
    PATCH_COMMAND ${ZOOKEEPER_PATCH}
    CONFIGURE_COMMAND ${ZOOKEEPER_CONF}
    BUILD_COMMAND ${MAKE_EXE} ${MAKEOPTS}
    INSTALL_COMMAND ${MAKE_EXE} install)
endif()

ExternalProject_Add(
  zookeeper-install-src
  URL ${ZOOKEEPER_URL}
  URL_HASH SHA256=${ZOOKEEPER_HASH}
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/zookeeper-ins
  DOWNLOAD_NO_EXTRACT TRUE
  INSTALL_DIR ${SRC_INSTALL_DIR}
  PATCH_COMMAND ""
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ""
  INSTALL_COMMAND bash -c "tar xzf <DOWNLOADED_FILE> -C ${SRC_INSTALL_DIR}")
