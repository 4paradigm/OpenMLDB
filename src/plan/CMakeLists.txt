include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)


add_library(fesql_planner STATIC planner.cc)
add_dependencies(fesql_node fesql_parser fesql_analyser)
add_executable(planner_test planner_test.cc)

target_link_libraries(planner_test  fesql_proto fesql_parser fesql_analyser
        fesql_node fesql_planner protobuf gtest ${COMMON_LIBS} pthread)
add_test(planner_test planner_test --gtest_output=xml:${CMAKE_BINARY_DIR}/planner_test.xml)
list(APPEND test_list planner_test)
