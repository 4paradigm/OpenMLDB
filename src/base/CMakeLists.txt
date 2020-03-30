include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_base STATIC fs_util.cc linenoise.cc texttable.cc graph.cc)

add_executable(hash_test hash_test.cc)
target_link_libraries(hash_test gflags gtest pthread)
add_test(hash_test hash_test --gtest_output=xml:${CMAKE_BINARY_DIR}/hash_test.xml)
list(APPEND test_list hash_test)

add_executable(graph_test graph_test.cc graph.cc)
target_link_libraries(graph_test fesql_base ${boost_libs} ${COMMON_LIBS} gtest pthread)
add_test(graph_test graph_test --gtest_output=xml:${CMAKE_BINARY_DIR}/graph_test.xml)
list(APPEND test_list graph_test)

add_executable(hash_mk hash_mk.cc)
target_link_libraries(hash_mk gflags benchmark benchmark_main pthread rt)

add_executable(texttable_test texttable_test.cc)
target_link_libraries(texttable_test fesql_base gflags gtest pthread)
add_test(texttable_test texttable_test --gtest_output=xml:${CMAKE_BINARY_DIR}/hash_test.xml)
list(APPEND test_list texttable_test)

add_executable(slice_test slice_test.cc)
target_link_libraries(slice_test gflags gtest pthread)
list(APPEND test_list slice_test)

