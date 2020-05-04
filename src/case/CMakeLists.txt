include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_case STATIC sql_case.cc)
set(case_deps_libs fesql_case ${yaml_libs} boost_filesystem ${VM_LIBS} ${LLVM_LIBS} ${BRPC_LIBS} ${OS_LIB})
add_executable(sql_case_test sql_case_test.cc)
target_link_libraries(sql_case_test gtest ${case_deps_libs})
add_test(sql_case_test sql_case_test --gtest_output=xml:${CMAKE_BINARY_DIR}/sql_case_test.xml)
list(APPEND test_list sql_case_test)

