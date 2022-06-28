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
set(OPENSSL_URL https://github.com/openssl/openssl/archive/refs/tags/OpenSSL_1_1_1o.tar.gz)
set(OPENSSL_HASH 0f745b85519aab2ce444a3dcada93311ba926aea2899596d01e7f948dbd99981)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "(arm64)|(ARM64)|(aarch64)|(AARCH64)")
  # in case error: '__NR_eventfd' undeclared
  set(OPENSSL_FLAGS no-shared no-afalgeng)
else()
  set(OPENSSL_FLAGS no-shared)
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
  CONFIGURE_COMMAND bash -c "${CONFIGURE_OPTS} ./config --prefix=<INSTALL_DIR> --openssldir=<INSTALL_DIR> ${OPENSSL_FLAGS}"
  BUILD_COMMAND ${MAKE_EXE} ${MAKEOPTS}
  INSTALL_COMMAND ${MAKE_EXE} install
)
