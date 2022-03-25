# Copyright 2022 4Paradigm
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

cmake_minimum_required(VERSION 3.13)

project(query-response-time)

find_package(absl REQUIRED)
find_package(GTest REQUIRED)
find_library(GLOG_LIBRARY glog)

set(LINK_LIBS absl::time absl::random_random ${GTEST_LIBRARIES} ${GLOG_LIBRARY})
link_libraries(${LINK_LIBS})

add_library(query_response_time STATIC ${CMAKE_CURRENT_SOURCE_DIR}/query_response_time.cc)

if(TESTING_ENABLE)
  add_executable(query_response_time_test ${CMAKE_CURRENT_SOURCE_DIR}/query_response_time_test.cc)
  target_link_libraries(query_response_time_test query_response_time)
  set_target_properties(
    query_response_time_test
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  )

  add_test(
    query_response_time_test
    ${CMAKE_CURRENT_BINARY_DIR}/query_response_time_test
    --gtest_output=xml:${CMAKE_CURRENT_BINARY_DIR}/query_response_time_test.xml
  )

  if(CMAKE_PROJECT_NAME STREQUAL "openmldb")
    list(APPEND test_list query_response_time_test)
    set(test_list ${test_list} PARENT_SCOPE)
  endif()
endif()
