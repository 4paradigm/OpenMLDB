include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)


add_executable(planner_test planner_test.cc planner.cc)
target_link_libraries(planner_test gtest glog)

