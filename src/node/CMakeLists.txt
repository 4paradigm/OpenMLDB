include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_node STATIC node_memory.cc sql_node.cc plan_node.cc)

add_executable(node_memory_test node_memory_test.cc node_memory.cc)
target_link_libraries(node_memory_test fesql_node glog gtest pthread)
add_test(node_memory_test node_memory_test --gtest_output=xml:${CMAKE_BINARY_DIR}/node_memory_test.xml)
list(APPEND test_list node_memory_test)



add_executable(sql_node_test sql_node_test.cc)
target_link_libraries(sql_node_test fesql_node glog gtest pthread)
add_test(sql_node_test sql_node_test --gtest_output=xml:${CMAKE_BINARY_DIR}/sql_node_test.xml)
list(APPEND test_list sql_node_test)

add_executable(sql_list_node_test sql_list_node_test.cc)
target_link_libraries(sql_list_node_test fesql_node glog gtest pthread)
add_test(sql_list_node_test sql_list_node_test --gtest_output=xml:${CMAKE_BINARY_DIR}/sql_list_node_test.xml)
list(APPEND test_list sql_list_node_test)


add_executable(plan_node_test plan_node_test.cc)
target_link_libraries(plan_node_test fesql_node gtest glog pthread)
add_test(plan_node_test plan_node_test --gtest_output=xml:${CMAKE_BINARY_DIR}/plan_node_test.xml)
list(APPEND test_list plan_node_test)
