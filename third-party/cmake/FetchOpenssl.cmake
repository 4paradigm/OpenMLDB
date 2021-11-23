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


find_program(MAKE_EXE NAMES gmake nmake make REQUIRED)
if (CMAKE_SYSTEM_PROCESSOR MATCHES "(arm64)|(ARM64)|(aarch64)|(AARCH64)")
  set(OPENSSL_URL https://github.com/openssl/openssl/archive/refs/tags/OpenSSL_1_1_1k.tar.gz)
  set(OPENSSL_HASH b92f9d3d12043c02860e5e602e50a73ed21a69947bcc74d391f41148e9f6aa95)

  set(OPENSSL_FLAGS no-shared no-afalgeng)
else()
  set(OPENSSL_URL https://github.com/openssl/openssl/archive/OpenSSL_1_1_0.zip)
  set(OPENSSL_HASH 2150919847ae685b58ae26e737f7e3aff82a934d793c3aee5a01cfc9ef55c121)

  set(OPENSSL_FLAGS no-shared)
  set(OPENSSL_PATCH bash -c "sed -i'' -e 's#qw/glob#qw/:glob#' Configure"
    COMMAND bash -c "sed -i'' -e 's#qw/glob#qw/:glob#' test/build.info")
endif()

message(STATUS "build openssl from ${OPENSSL_URL}")

ExternalProject_Add(
  openssl
  URL ${OPENSSL_URL}
  URL_HASH SHA256=${OPENSSL_HASH}
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/openssl
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  BUILD_IN_SOURCE True
  PATCH_COMMAND ${OPENSSL_PATCH}
  CONFIGURE_COMMAND ./config --prefix=<INSTALL_DIR> --openssldir=<INSTALL_DIR> ${OPENSSL_FLAGS}
  BUILD_COMMAND ${MAKE_EXE} ${MAKEOPTS}
  INSTALL_COMMAND ${MAKE_EXE} install
    COMMAND bash -c "rm -rvf <INSTALL_DIR>/lib/libssl.so*"
    COMMAND bash -c "rm -rvf <INSTALL_DIR>/lib/libcrypto.so*")
