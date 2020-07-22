include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_base STATIC fe_slice.cc fs_util.cc fe_linenoise.cc texttable.cc graph.cc mem_pool.h)

if (TESTING_ENABLE)
    add_executable(hash_test hash_test.cc)
    target_link_libraries(hash_test gflags gtest pthread)
    add_test(hash_test hash_test --gtest_output=xml:${CMAKE_BINARY_DIR}/hash_test.xml)
    list(APPEND test_list hash_test)

    add_executable(mem_pool_test mem_pool_test.cc)
    target_link_libraries(mem_pool_test gflags gtest pthread)
    add_test(mem_pool_test mem_pool_test --gtest_output=xml:${CMAKE_BINARY_DIR}/mem_pool_test.xml)
    list(APPEND test_list mem_pool_test)

    add_executable(graph_test graph_test.cc graph.cc)
    target_link_libraries(graph_test fesql_base ${boost_libs} glog gflags gtest pthread)
    add_test(graph_test graph_test --gtest_output=xml:${CMAKE_BINARY_DIR}/graph_test.xml)
    list(APPEND test_list graph_test)

    add_executable(hash_mk hash_mk.cc)
    target_link_libraries(hash_mk gflags benchmark benchmark_main ${OS_LIB})

    add_executable(texttable_test texttable_test.cc)
    target_link_libraries(texttable_test fesql_base gflags gtest pthread)
    add_test(texttable_test texttable_test --gtest_output=xml:${CMAKE_BINARY_DIR}/hash_test.xml)
    list(APPEND test_list texttable_test)

    add_executable(slice_test fe_slice_test.cc)
    target_link_libraries(slice_test fesql_base gflags gtest pthread)
    list(APPEND test_list slice_test)
endif ()


