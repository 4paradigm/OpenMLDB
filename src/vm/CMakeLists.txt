include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)

llvm_map_components_to_libnames(llvm_libs support core orcjit nativecodegen)

add_library(fesql_vm STATIC jit.cc)

llvm_map_components_to_libnames(llvm_libs support core orcjit nativecodegen)
add_executable(jit_test jit_test.cc)
target_link_libraries(jit_test fesql_vm  ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(jit_test jit_test --gtest_output=xml:${CMAKE_BINARY_DIR}/jit_test.xml)
list(APPEND test_list jit_test)


