#[[
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

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
add_library(hybridse_planner_v2 STATIC
        planner_v2.cc ast_node_converter.cc)
target_link_libraries(hybridse_planner_v2 ${ZETASQL_LIBS} hybridse_core glog)


add_executable(ast_node_converter_test ast_node_converter_test.cc)
target_link_libraries(ast_node_converter_test ${GTEST_LIBRARIES} hybridse_planner_v2 hybridse_core ${GLOG_LIBRARY}
        ${ZETASQL_LIBS} ${GTEST_LIBRARIES})
add_test(ast_node_converter_test ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ast_node_converter_test --gtest_output=xml:${CMAKE_BINARY_DIR}/ast_node_converter_test.xml)
list(APPEND test_list planner_v2_test)

add_executable(planner_v2_test planner_v2_test.cc)
target_link_libraries(planner_v2_test ${GTEST_LIBRARIES} hybridse_planner_v2 hybridse_core ${GLOG_LIBRARY}
        ${ZETASQL_LIBS} ${GTEST_LIBRARIES})

add_test(planner_v2_test ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/planner_v2_test --gtest_output=xml:${CMAKE_BINARY_DIR}/planner_v2_test.xml)
list(APPEND test_list planner_v2_test)
]]
