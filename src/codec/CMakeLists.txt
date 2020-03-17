include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)

add_library(fesql_codec STATIC type_codec.cc row_codec.cc)

add_executable(schema_codec_test schema_codec_test.cc)
target_link_libraries(schema_codec_test fesql_proto protobuf gflags gtest pthread)
add_test(schema_codec_test schema_codec_test --gtest_output=xml:${CMAKE_BINARY_DIR}/schema_codec_test.xml)
list(APPEND test_list schema_codec_test)

add_executable(row_codec_test row_codec_test.cc)
target_link_libraries(row_codec_test fesql_codec fesql_proto protobuf glog gflags unwind lzma gtest pthread)
add_test(row_codec_test row_codec_test --gtest_output=xml:${CMAKE_BINARY_DIR}/row_codec_test.xml)
list(APPEND test_list row_codec_test)

add_executable(window_test window_test.cc)
target_link_libraries(window_test fesql_codec fesql_proto protobuf glog gflags unwind lzma gtest pthread)
add_test(window_test window_test --gtest_output=xml:${CMAKE_BINARY_DIR}/window_test.xml)
list(APPEND test_list window_test)
