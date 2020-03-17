include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_dbms STATIC dbms_server_impl.cc)

add_executable(dbms_server_test dbms_server_impl_test.cc)
target_link_libraries(dbms_server_test fesql_dbms fesql_tablet
    fesql_storage fesql_vm fesql_parser fesql_planner fesql_codegen
    fesql_proto fesql_udf fesql_node fesql_codec
    brpc protobuf gtest ${COMMON_LIBS} ${llvm_libs} leveldb z snappy ssl crypto dl pthread)
add_test(dbms_server_test dbms_server_test --gtest_output=xml:${CMAKE_BINARY_DIR}/dbms_server_test.xml)
list(APPEND test_list dbms_server_test)

