include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)

add_library(fesql_codec STATIC type_codec.cc row_codec.cc list_iterator_codec.cc row.cc)
set(case_deps_libs fesql_case ${yaml_libs} boost_filesystem)


add_executable(row_test row_test.cc)
target_link_libraries(row_test fesql_codec ${case_deps_libs} fesql_proto ${COMMON_TEST_LIBS})
add_test(row_test row_test --gtest_output=xml:${CMAKE_BINARY_DIR}/row_test.xml)
list(APPEND test_list row_test)

add_executable(schema_codec_test schema_codec_test.cc)
target_link_libraries(schema_codec_test fesql_proto ${COMMON_TEST_LIBS})
add_test(schema_codec_test schema_codec_test --gtest_output=xml:${CMAKE_BINARY_DIR}/schema_codec_test.xml)
list(APPEND test_list schema_codec_test)

add_executable(row_codec_test row_codec_test.cc)
target_link_libraries(row_codec_test fesql_codec fesql_proto ${COMMON_TEST_LIBS})
add_test(row_codec_test row_codec_test --gtest_output=xml:${CMAKE_BINARY_DIR}/row_codec_test.xml)
list(APPEND test_list row_codec_test)

