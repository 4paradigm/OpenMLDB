include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_node STATIC node_manager.cc sql_node.cc plan_node.cc)

add_executable(node_manager_test node_manager_test.cc node_manager.cc)
target_link_libraries(node_manager_test fesql_node ${COMMON_LIBS} gtest pthread)
add_test(node_manager_test node_manager_test --gtest_output=xml:${CMAKE_BINARY_DIR}/node_manager_test.xml)
list(APPEND test_list node_manager_test)

add_executable(sql_node_test sql_node_test.cc)
target_link_libraries(sql_node_test fesql_node ${COMMON_LIBS} gtest pthread)
add_test(sql_node_test sql_node_test --gtest_output=xml:${CMAKE_BINARY_DIR}/sql_node_test.xml)
list(APPEND test_list sql_node_test)

add_executable(plan_node_test plan_node_test.cc)
target_link_libraries(plan_node_test fesql_node gtest ${COMMON_LIBS} pthread)
add_test(plan_node_test plan_node_test --gtest_output=xml:${CMAKE_BINARY_DIR}/plan_node_test.xml)
list(APPEND test_list plan_node_test)
