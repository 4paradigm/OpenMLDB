include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_executable(plan_node_test plan_node_test.cc plan_node.cc)
target_link_libraries(plan_node_test gtest glog)
add_test(plan_node_test plan_node_test --gtest_output=xml:${CMAKE_BINARY_DIR}/plan_node_test.xml)

add_executable(planner_test planner_test.cc planner.cc ${PROJECT_SOURCE_DIR}/src/parser/node.cc)
target_link_libraries(planner_test gtest glog)
add_test(planner_test planner_test --gtest_output=xml:${CMAKE_BINARY_DIR}/planner_test.xml)

if (COVERAGE_ENABLE)
    if(CMAKE_COMPILER_IS_GNUCXX)
        SETUP_TARGET_FOR_COVERAGE_LCOV(NAME plan_test_coverage EXECUTABLE plan_node_test coverage)
    endif()
endif()
