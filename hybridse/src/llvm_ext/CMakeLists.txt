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

message(STATUS "Enable hybridse llvm ext sources")
add_library(hybridse_llvm_ext STATIC symbol_resolve.cc)
if (NOT ${LLVM_ENABLE_RTTI})
    message(STATUS "Compile hybridse llvm ext with -fno-rtti")
    target_compile_options(hybridse_llvm_ext PRIVATE -fno-rtti)
endif()
target_link_libraries(hybridse_llvm_ext ${LLVM_LIBS})
set(LLVM_EXT_LIB hybridse_llvm_ext PARENT_SCOPE)
