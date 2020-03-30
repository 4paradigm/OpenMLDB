include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)

add_library(fesql_storage STATIC table.cc segment.cc table_iterator.cc)

add_executable(table_iterator_test table_iterator_test.cc)
target_link_libraries(table_iterator_test 
fesql_storage fesql_codec fesql_proto glog gflags gtest protobuf pthread
unwind lzma)
add_test(table_iterator_test table_iterator_test --gtest_output=xml:${CMAKE_BINARY_DIR}/table_iterator_test.xml)
list(APPEND test_list table_iterator_test)

add_executable(list_test list_test.cc)
target_link_libraries(list_test gflags gtest pthread)
add_test(list_test list_test --gtest_output=xml:${CMAKE_BINARY_DIR}/list_test.xml)
list(APPEND test_list list_test)

add_executable(skiplist_test skiplist_test.cc)
target_link_libraries(skiplist_test gflags gtest pthread)
add_test(skiplist_test skiplist_test --gtest_output=xml:${CMAKE_BINARY_DIR}/skiplist_test.xml)
list(APPEND test_list skiplist_test)

add_executable(table_test table_test.cc)
target_link_libraries(table_test fesql_storage fesql_codec fesql_proto glog
    gflags gtest protobuf pthread unwind lzma)
add_test(table_test table_test --gtest_output=xml:${CMAKE_BINARY_DIR}/table_test.xml)
list(APPEND test_list table_test)

