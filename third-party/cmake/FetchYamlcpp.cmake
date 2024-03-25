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

set(YAMLCPP_URL https://github.com/jbeder/yaml-cpp/archive/refs/tags/yaml-cpp-0.6.3.tar.gz)

message(STATUS "build yaml-cpp from ${YAMLCPP_URL}")

ExternalProject_Add(
  yaml-cpp
  URL ${YAMLCPP_URL}
  URL_HASH SHA256=77ea1b90b3718aa0c324207cb29418f5bced2354c2e483a9523d98c3460af1ed
  PREFIX ${DEPS_BUILD_DIR}
  DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/yaml-cpp
  INSTALL_DIR ${DEPS_INSTALL_DIR}
  CONFIGURE_COMMAND ${CMAKE_COMMAND} -H<SOURCE_DIR> -B<BINARY_DIR> -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR> ${CMAKE_OPTS}
  BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR>
  INSTALL_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --target install)
