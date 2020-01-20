include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)


add_executable(base_bm_test base_bm_test.cc)
target_link_libraries(base_bm_test gflags gtest pthread)
add_test(base_bm_test base_bm_test --gtest_output=xml:${CMAKE_BINARY_DIR}/base_bm_test.xml)
list(APPEND test_list base_bm_test)

#add_executable(memsql_client_bm memsql_client_bm.cc)
#target_link_libraries(memsql_client_bm benchmark mysqlclient ${COMMON_LIBS} gtest pthread)


add_library(fesql_client_bm_case STATIC fesql_client_bm_case.cc)

add_executable(fesql_client_bm_case_test fesql_client_bm_case_test.cc)
target_link_libraries(fesql_client_bm_case_test fesql_client_bm_case
        fesql_sdk
        fesql_dbms
        fesql_tablet
        fesql_storage fesql_vm fesql_analyser fesql_parser fesql_planner fesql_codegen fesql_udf
        fesql_node
        fesql_proto brpc protobuf benchmark gtest ${COMMON_LIBS} ${llvm_libs} leveldb z snappy ssl crypto dl pthread)

add_test(fesql_client_bm_case_test fesql_client_bm_case_test --gtest_output=xml:${CMAKE_BINARY_DIR}/fesql_client_bm_case_test.xml)
list(APPEND test_list fesql_client_bm_case_test)

add_executable(fesql_client_bm fesql_client_bm.cc)
target_link_libraries(fesql_client_bm
        fesql_client_bm_case
        fesql_sdk
        fesql_dbms
        fesql_tablet
        fesql_storage fesql_vm fesql_analyser fesql_parser fesql_planner fesql_codegen fesql_udf
        fesql_node
        fesql_proto brpc protobuf benchmark gtest ${COMMON_LIBS} ${llvm_libs} leveldb z snappy ssl crypto dl pthread)

add_executable(fesql_client_batch_run_bm fesql_client_batch_run_bm.cc)
target_link_libraries(fesql_client_batch_run_bm
        fesql_client_bm_case
        fesql_sdk
        fesql_dbms
        fesql_tablet
        fesql_storage fesql_vm fesql_analyser fesql_parser fesql_planner fesql_codegen fesql_udf
        fesql_node
        fesql_proto brpc protobuf benchmark gtest ${COMMON_LIBS} ${llvm_libs} leveldb z snappy ssl crypto dl pthread)
