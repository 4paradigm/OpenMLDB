include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)
add_library(fesql_udf SHARED udf.cc)
add_executable(udf_test udf_test.cc)
target_link_libraries(udf_test fesql_udf fesql_storage fesql_vm fesql_udf fesql_proto protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(udf_test udf_test --gtest_output=xml:${CMAKE_BINARY_DIR}/udf_test.xml)
list(APPEND test_list udf_test)
