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

set(LLVM_URL https://github.com/llvm/llvm-project/releases/download/llvmorg-9.0.1/llvm-9.0.1.src.tar.xz)
message(STATUS "build llvm from ${LLVM_URL}")

if (CMAKE_SYSTEM_PROCESSOR MATCHES "(arm64)|(ARM64)|(aarch64)|(AARCH64)")
  set(LLVM_TARGETS AArch64)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  set(LLVM_TARGETS X86)
else()
  set(LLVM_TARGETS all)
endif()

ExternalProject_Add(
  llvm
  URL ${LLVM_URL}
  URL_HASH SHA256=00a1ee1f389f81e9979f3a640a01c431b3021de0d42278f6508391a2f0b81c9a
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/llvm
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  CONFIGURE_COMMAND ${CMAKE_COMMAND} -H<SOURCE_DIR> -B<BINARY_DIR>
    -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR> -DLLVM_TARGETS_TO_BUILD=${LLVM_TARGETS}
    -DCMAKE_CXX_FLAGS=-fPIC -DLLVM_ENABLE_Z3_SOLVER=OFF
  BUILD_COMMAND ${CMAKE_COMMAND} --build . -- ${MAKEOPTS}
  INSTALL_COMMAND ${CMAKE_COMMAND} --build . --target install)
