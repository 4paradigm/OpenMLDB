include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_sdk STATIC dbms_sdk.cc tablet_sdk.cc)
add_dependencies(fesql_sdk fesql_node fesql_parser fesql_analyser fesql_planner fesql_proto)

add_executable(tablet_sdk_test tablet_sdk_test.cc)
target_link_libraries(tablet_sdk_test ${FESQL_LIBS} brpc protobuf gtest ${COMMON_LIBS} ${llvm_libs} leveldb z snappy ssl crypto dl pthread)
add_test(tablet_sdk_test tablet_sdk_test
    --gtest_output=xml:${CMAKE_BINARY_DIR}/tablet_sdk_test.xml)
list(APPEND test_list tablet_sdk_test)
