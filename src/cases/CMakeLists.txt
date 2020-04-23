include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_cases STATIC sql_case.cc)
add_executable(sql_case_test sql_case_test.cc)
target_link_libraries(sql_case_test fesql_cases fesql_vm fesql_proto protobuf glog gflags lzma gtest pthread)
add_test(sql_case_test sql_case_test --gtest_output=xml:${CMAKE_BINARY_DIR}/sql_case_test.xml)
list(APPEND test_list sql_case_test)

