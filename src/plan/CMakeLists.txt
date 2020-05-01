include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_planner STATIC planner.cc)

set(case_deps_libs fesql_case ${yaml_libs} fesql_codec boost_filesystem)

add_executable(planner_test planner_test.cc)
target_link_libraries(planner_test ${case_deps_libs} fesql_proto fesql_parser
        fesql_node fesql_planner brpc protobuf gtest ${COMMON_LIBS} ssl crypto dl pthread)
add_test(planner_test planner_test --gtest_output=xml:${CMAKE_BINARY_DIR}/planner_test.xml)
list(APPEND test_list planner_test)

