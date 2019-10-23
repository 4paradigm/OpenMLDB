include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)

llvm_map_components_to_libnames(llvm_libs support core orcjit nativecodegen)

add_library(fesql_codegen STATIC flatbuf_ir_builder.cc)
add_executable(ir_base_builder_test ir_base_builder_test.cc)
target_link_libraries(ir_base_builder_test gflags ${llvm_libs} glog gtest pthread)
add_test(ir_base_builder_test ir_base_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/ir_base_builder_test.xml)

add_executable(flatbuf_ir_builder_test flatbuf_ir_builder_test.cc)
target_link_libraries(flatbuf_ir_builder_test gflags fesql_codegen fesql_proto ${llvm_libs} protobuf glog gtest pthread)
add_test(flatbuf_ir_builder_test flatbuf_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/flatbuf_ir_builder_test.xml)


