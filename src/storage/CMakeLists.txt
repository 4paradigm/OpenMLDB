include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)
add_executable(list_test list_test.cc)
target_link_libraries(list_test gflags gtest pthread)

add_executable(skiplist_test skiplist_test.cc)
target_link_libraries(skiplist_test gflags gtest pthread)

add_executable(table_test table_test.cc table.cc segment.cc)
target_link_libraries(table_test gflags gtest pthread)
