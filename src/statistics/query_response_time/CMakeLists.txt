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

set(LINK_LIBS absl::time absl::random_random absl::strings absl::status absl::statusor absl::synchronization ${BRPC_LIBS} ${GTEST_LIBRARIES} ${GLOG_LIBRARY} ${GFLAGS_LIBRARY})
link_libraries(${LINK_LIBS})

if(CMAKE_CXX_COMPILER_ID MATCHES "(AppleClang)|(Clang)")
  add_definitions(-Wthread-safety)
endif()

add_library(query_response_time STATIC ${CMAKE_CURRENT_SOURCE_DIR}/deployment_metric_collector.cc)

function(add_test_file TARGET_NAME SOURCE_NAME)
  add_executable(${TARGET_NAME} ${SOURCE_NAME})
  target_link_libraries(${TARGET_NAME} query_response_time)
  set_target_properties(
    ${TARGET_NAME}
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  )

  add_test(
    ${TARGET_NAME}
    ${CMAKE_CURRENT_BINARY_DIR}/${TARGET_NAME}
    --gtest_output=xml:${CMAKE_CURRENT_BINARY_DIR}/${TARGET_NAME}.xml
  )
  list(APPEND test_list ${TARGET_NAME})
  set(test_list ${test_list} PARENT_SCOPE)
endfunction(add_test_file)

if(TESTING_ENABLE)
  add_test_file(deployment_metric_collector_test ${CMAKE_CURRENT_SOURCE_DIR}/deployment_metric_collector_test.cc)

  if(CMAKE_PROJECT_NAME STREQUAL "openmldb")
    set(test_list ${test_list} PARENT_SCOPE)
  endif()
endif()
