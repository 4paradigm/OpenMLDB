include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)
add_executable(hash_test hash_test.cc)
target_link_libraries(hash_test gflags gtest pthread)
add_test(hash_test hash_test)

if (COVERAGE_ENABLE)
    if(CMAKE_COMPILER_IS_GNUCXX)
        SETUP_TARGET_FOR_COVERAGE_LCOV(NAME hash_test_coverage  EXECUTABLE hash_test coverage)
    endif()
endif()

llvm_map_components_to_libnames(llvm_libs support core orcjit nativecodegen)
add_executable(hash_mk hash_mk.cc)
target_link_libraries(hash_mk gflags benchmark benchmark_main pthread)

add_executable(flat_mk flat_mk.cc)
target_link_libraries(flat_mk gflags ${llvm_libs} flatbuffers benchmark benchmark_main jit pthread)

add_executable(lljit lljit.cc)
target_link_libraries(lljit ${llvm_libs} flatbuffers)

