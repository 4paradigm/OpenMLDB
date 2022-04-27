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


set(NLOHMANNJSON_URL https://github.com/nlohmann/json/archive/refs/tags/v3.10.5.zip)

message(STATUS "build nlohmann_json from ${NLOHMANNJSON_URL}")

find_program(MAKE_EXE NAMES gmake nmake make REQUIRED)
#find_program(TCLSH NAMES tclsh UIRED)

ExternalProject_Add(
        nlohmann_json
        URL ${NLOHMANNJSON_URL}
        URL_HASH SHA256=ea4b0084709fb934f92ca0a68669daa0fe6f2a2c6400bf353454993a834bb0bb
        PREFIX ${DEPS_BUILD_DIR}
        DOWNLOAD_DIR ${DEPS_DOWNLOAD_DIR}/nlohmann_json
        INSTALL_DIR ${DEPS_INSTALL_DIR}
        BUILD_COMMAND ${MAKE_EXE} ${MAKEOPTS}
        INSTALL_COMMAND ${MAKE_EXE} install
)

