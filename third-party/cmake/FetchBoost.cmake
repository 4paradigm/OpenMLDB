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

set(BOOST_URL https://archives.boost.io/release/1.83.0/source/boost_1_83_0.tar.gz)

message(STATUS "build boost from ${BOOST_URL}")

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(BOOST_TOOLSET clang)
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(BOOST_TOOLSET gcc)
else()
    message(FATAL_ERROR "Unsupported compiler for Boost build")
endif()

# boost require python development package, python-dev on debian or python-devel on redhat
ExternalProject_Add(
  boost
  URL ${BOOST_URL}
  URL_HASH SHA256=c0685b68dd44cc46574cce86c4e17c0f611b15e195be9848dfd0769a0a207628
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/boost
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  BUILD_IN_SOURCE True
  CONFIGURE_COMMAND ./bootstrap.sh ${BOOST_FLAGS}
  BUILD_COMMAND ./b2 toolset=${BOOST_TOOLSET} link=static cxxstd=17 cxxflags=-fPIC cflags=-fPIC --without-python release install --prefix=<INSTALL_DIR>
  INSTALL_COMMAND "")
