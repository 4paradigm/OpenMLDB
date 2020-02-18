include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)
add_library(fesql_storage STATIC table.cc segment.cc codec.cc type_native_fn.cc window.cc table_iterator.cc)

add_executable(codec_test codec_test.cc)
target_link_libraries(codec_test fesql_proto fesql_codegen fesql_vm
    fesql_storage glog gflags ${llvm_libs} gtest protobuf pthread
    unwind lzma)

add_executable(list_test list_test.cc)
target_link_libraries(list_test gflags gtest pthread)
list(APPEND test_list list_test)

add_executable(skiplist_test skiplist_test.cc)
target_link_libraries(skiplist_test gflags gtest pthread)
list(APPEND test_list skiplist_test)

add_executable(table_test table_test.cc)
target_link_libraries(table_test fesql_storage fesql_proto fesql_vm glog
    ${llvm_libs} gflags gtest protobuf pthread unwind lzma)
list(APPEND test_list table_test)

add_executable(window_test window_test.cc)
target_link_libraries(window_test fesql_proto fesql_codegen fesql_vm fesql_storage glog gflags ${llvm_libs} gtest pthread unwind lzma)
add_test(window_test window_test --gtest_output=xml:${CMAKE_BINARY_DIR}/window_test.xml)
list(APPEND test_list window_test)
