include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_dbms STATIC dbms_server_impl.cc)
add_dependencies(fesql_dbms fesql_proto)


#add_executable(fesql_server_dbms_test dbms_server_impl_test.cc)
#target_link_libraries(fesql_server_dbms_test fesql_dbms fesql_base fesql_proto fesql_node brpc protobuf ${COMMON_LIBS} gtest pthread)
#add_test(fesql_server_dbms_test fesql_server_dbms_test --gtest_output=xml:${CMAKE_BINARY_DIR}/fesql_server_dbms_test.xml)
#list(APPEND test_list fesql_server_dbms_test)

