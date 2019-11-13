include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_sdk STATIC dbms_sdk.cc)
add_dependencies(fesql_sdk fesql_node fesql_parser fesql_analyser fesql_planner fesql_proto)

#add_executable(dbms_sdk_test dbms_sdk_test.cc)
#target_link_libraries(dbms_sdk_test fesql_dbms fesql_sdk fesql_proto fesql_node fesql_parser fesql_analyser fesql_planner brpc
#        protobuf gtest ${COMMON_LIBS} pthread)
#add_test(dbms_sdk_test dbms_sdk_test --gtest_output=xml:${CMAKE_BINARY_DIR}/dbms_sdk_test.xml)
#list(APPEND test_list dbms_sdk_test)