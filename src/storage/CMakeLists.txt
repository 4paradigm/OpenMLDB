include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)
add_executable(list_test list_test.cc)
target_link_libraries(list_test gflags gtest pthread)
add_test(list_test --gtest_output=xml:${CMAKE_BINARY_DIR}/list_test.xml)
