include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_planner STATIC planner.cc)

add_test(planner_test planner_test --gtest_output=xml:${CMAKE_BINARY_DIR}/planner_test.xml)
list(APPEND test_list planner_test)
