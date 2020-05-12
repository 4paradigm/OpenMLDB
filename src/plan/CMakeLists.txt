include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_planner STATIC planner.cc)

if (TESTING_ENABLE)
set(case_deps_libs fesql_case ${yaml_libs} boost_filesystem ${VM_LIBS} ${LLVM_LIBS} ${BRPC_LIBS} ${OS_LIB})

add_executable(planner_test planner_test.cc)
target_link_libraries(planner_test gtest ${case_deps_libs})
add_test(planner_test planner_test --gtest_output=xml:${CMAKE_BINARY_DIR}/planner_test.xml)
list(APPEND test_list planner_test)
endif()
