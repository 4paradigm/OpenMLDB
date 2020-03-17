include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_sdk STATIC dbms_sdk.cc tablet_sdk.cc result_set_impl.cc)

add_executable(dbms_sdk_test dbms_sdk_test.cc)
target_link_libraries(dbms_sdk_test fesql_sdk 
    fesql_dbms
    fesql_tablet 
    fesql_storage fesql_vm fesql_analyser fesql_parser fesql_planner fesql_codegen fesql_udf
    fesql_node
    fesql_codec fesql_proto brpc protobuf gtest ${COMMON_LIBS} ${llvm_libs} leveldb z snappy ssl crypto dl pthread)
add_test(dbms_sdk_test dbms_sdk_test --gtest_output=xml:${CMAKE_BINARY_DIR}/dbms_sdk_test.xml)
list(APPEND test_list dbms_sdk_test)

add_executable(tablet_sdk_test tablet_sdk_test.cc)
target_link_libraries(tablet_sdk_test fesql_sdk 
    fesql_dbms
    fesql_tablet 
    fesql_storage fesql_vm fesql_analyser fesql_parser fesql_planner fesql_codegen fesql_udf
    fesql_node
    fesql_codec fesql_proto brpc protobuf gtest ${COMMON_LIBS} ${llvm_libs} leveldb z snappy ssl crypto dl pthread)
add_test(tablet_sdk_test tablet_sdk_test
    --gtest_output=xml:${CMAKE_BINARY_DIR}/tablet_sdk_test.xml)
list(APPEND test_list tablet_sdk_test)

add_executable(result_set_impl_test result_set_impl_test.cc)
target_link_libraries(result_set_impl_test  fesql_sdk
    fesql_tablet 
    fesql_storage fesql_vm fesql_analyser fesql_parser fesql_planner fesql_codegen fesql_udf
    fesql_node
    fesql_codec fesql_proto brpc protobuf gtest ${COMMON_LIBS} ${llvm_libs} leveldb z snappy ssl crypto dl pthread)
add_test(result_set_impl_test result_set_impl_test --gtest_output=xml:${CMAKE_BINARY_DIR}/result_set_impl_test.xml)
list(APPEND test_list result_set_impl_test)
