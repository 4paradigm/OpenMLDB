include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)


add_library(fesql_analyser STATIC analyser.cc)
add_executable(analyser_test analyser_test.cc)
target_link_libraries(analyser_test fesql_analyser fesql_proto fesql_parser fesql_node protobuf gtest glog pthread)
add_test(analyser_test analyser_test --gtest_output=xml:${CMAKE_BINARY_DIR}/analyser_test.xml)
list(APPEND test_list analyser_test)
