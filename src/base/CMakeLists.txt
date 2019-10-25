include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)
add_executable(hash_test hash_test.cc)
target_link_libraries(hash_test gflags gtest pthread)
add_test(hash_test hash_test --gtest_output=xml:${CMAKE_BINARY_DIR}/hash_test.xml)
list(APPEND test_list hash_test)

llvm_map_components_to_libnames(llvm_libs support core orcjit nativecodegen)
add_executable(hash_mk hash_mk.cc)
target_link_libraries(hash_mk gflags benchmark benchmark_main pthread)

add_executable(flat_mk flat_mk.cc)
target_link_libraries(flat_mk gflags ${llvm_libs} flatbuffers benchmark benchmark_main jit pthread)

