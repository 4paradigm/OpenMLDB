include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)
add_library(fesql_storage STATIC table.cc segment.cc codec.cc type_ir_builder.cc)

add_executable(codec_test codec_test.cc codec.cc type_ir_builder.cc)
target_link_libraries(codec_test fesql_proto fesql_codegen fesql_vm
    fesql_storage glog gflags ${llvm_libs} gtest protobuf pthread
    unwind lzma)

add_executable(list_test list_test.cc)
target_link_libraries(list_test gflags gtest pthread)

add_executable(skiplist_test skiplist_test.cc)
target_link_libraries(skiplist_test gflags gtest pthread)

add_executable(table_test table_test.cc)
target_link_libraries(table_test fesql_storage fesql_proto glog gflags gtest protobuf pthread unwind)
