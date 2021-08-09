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

include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(hybridse_test_base STATIC engine_test_base.cc test_base.cc)
target_link_libraries(hybridse_test_base ${HYBRIDSE_CORE_LIBS}
        ${yaml_libs} ${LLVM_LIBS} ${OS_LIB} ${COMMON_LIBS} ${g_libs} ${LLVM_EXT_LIB} hybridse_flags)
