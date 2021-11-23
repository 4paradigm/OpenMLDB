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


set(SWIG_URL https://github.com/swig/swig/archive/v4.0.1.tar.gz)

message(STATUS "build swig from ${SWIG_URL}")

find_program(MAKE_EXE NAMES gmake nmake make REQUIRED)

ExternalProject_Add(
  swig
  URL ${SWIG_URL}
  URL_HASH SHA256=ace86bba4d99e40d178487796521de86b48356031381bdd026cf3acf40f22d9b
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/swig
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  BUILD_IN_SOURCE True
  CONFIGURE_COMMAND ./autogen.sh
    COMMAND ./configure --without-pcre --prefix=<INSTALL_DIR>
  BUILD_COMMAND ${MAKE_EXE} ${MAKEOPTS}
  INSTALL_COMMAND ${MAKE_EXE} install)
