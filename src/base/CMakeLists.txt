include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_executable(hash_test hash_test.cc)
target_link_libraries(hash_test gflags gtest pthread)
add_test(hash_test hash_test --gtest_output=xml:${CMAKE_BINARY_DIR}/hash_test.xml)
list(APPEND test_list hash_test)

add_executable(texttable_test texttable_test.cc)
target_link_libraries(texttable_test gflags gtest pthread)
add_test(texttable_test texttable_test --gtest_output=xml:${CMAKE_BINARY_DIR}/hash_test.xml)
list(APPEND test_list texttable_test)

llvm_map_components_to_libnames(llvm_libs support core orcjit nativecodegen)
add_executable(hash_mk hash_mk.cc)
target_link_libraries(hash_mk gflags benchmark benchmark_main pthread)
list(APPEND mk_list hash_mk)
add_library(fesql_base STATIC linenoise.cc)
