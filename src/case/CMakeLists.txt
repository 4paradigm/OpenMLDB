include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_case STATIC sql_case.cc)
set(case_deps_libs fesql_case ${yaml_libs} boost_filesystem)
add_executable(sql_case_test sql_case_test.cc)
target_link_libraries(sql_case_test ${case_deps_libs} fesql_vm fesql_codec fesql_proto ${COMMON_LIBS} gtest pthread)
add_test(sql_case_test sql_case_test --gtest_output=xml:${CMAKE_BINARY_DIR}/sql_case_test.xml)
list(APPEND test_list sql_case_test)

