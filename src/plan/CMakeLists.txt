include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_planer STATIC planner.cc plan_node.cc)

add_executable(plan_node_test plan_node_test.cc)
target_link_libraries(plan_node_test fesql_planer gtest glog pthread)
add_test(plan_node_test plan_node_test --gtest_output=xml:${CMAKE_BINARY_DIR}/plan_node_test.xml)
list(APPEND test_list plan_node_test)

add_executable(planner_test planner_test.cc) 
target_link_libraries(planner_test fesql_planer fesql_parser gtest glog pthread)
add_test(planner_test planner_test --gtest_output=xml:${CMAKE_BINARY_DIR}/planner_test.xml)
list(APPEND test_list planner_test)
