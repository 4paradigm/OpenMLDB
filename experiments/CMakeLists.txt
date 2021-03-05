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

add_executable(bitcode_test bitcode/bitcode_test.cc)
target_link_libraries(bitcode_test ${LLVM_LIBS}  gtest pthread)

add_executable(sql_parquet_test parquet/sql_parquet_demo.cc)
target_link_libraries(sql_parquet_test ${ARROW_LIBS} glog gflags  gtest pthread)

add_executable(boost_test boost/boost_test.cc)
target_link_libraries(boost_test ${boost_libs}  gtest pthread)

add_executable(io_buf_test iobuf/test_iobuf.cc)
target_link_libraries(io_buf_test gtest ${BRPC_LIBS} ${OS_LIB})
