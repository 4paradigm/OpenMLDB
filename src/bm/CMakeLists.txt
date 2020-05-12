include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

if (TESTING_ENABLE)
    set(BM_LIBS fesql_sdk fesql_dbms fesql_tablet fesql_flags ${VM_LIBS} ${LLVM_LIBS} ${BRPC_LIBS} tcmalloc ${OS_LIB})
    add_executable(base_bm_test base_bm_test.cc)


    target_link_libraries(base_bm_test gtest fesql_dbms fesql_tablet fesql_flags ${BM_LIBS})
    add_test(base_bm_test base_bm_test --gtest_output=xml:${CMAKE_BINARY_DIR}/base_bm_test.xml)
    list(APPEND test_list base_bm_test)

    add_library(storage_bm_case STATIC storage_bm_case.cc)
    add_executable(storage_bm_case_test storage_bm_case_test.cc)
    target_link_libraries(storage_bm_case_test storage_bm_case 
        benchmark gtest ${BM_LIBS})
    add_test(storage_bm_case_test storage_bm_case_test --gtest_output=xml:${CMAKE_BINARY_DIR}/storage_bm_case_test.xml)
    list(APPEND test_list storage_bm_case_test)
    add_executable(storage_bm storage_bm.cc)
    target_link_libraries(storage_bm storage_bm_case
        benchmark gtest ${BM_LIBS})

    add_library(udf_bm_case STATIC udf_bm_case.cc)
    add_executable(udf_bm_case_test udf_bm_case_test.cc)
    target_link_libraries(udf_bm_case_test udf_bm_case 
        benchmark gtest fesql_udf ${BM_LIBS})
    add_test(udf_bm_case_test udf_bm_case_test --gtest_output=xml:${CMAKE_BINARY_DIR}/udf_bm_case_test.xml)
    list(APPEND test_list udf_bm_case_test)

    add_executable(udf_bm udf_bm.cc)
    target_link_libraries(udf_bm udf_bm_case 
        benchmark gtest fesql_udf ${BM_LIBS})

    add_library(runner_bm_case STATIC runner_bm_case.cc)
    add_executable(runner_bm_case_test runner_bm_case_test.cc)
    target_link_libraries(runner_bm_case_test runner_bm_case
        benchmark gtest ${BM_LIBS})

    add_test(runner_bm_case_test runner_bm_case_test --gtest_output=xml:${CMAKE_BINARY_DIR}/runner_bm_case_test.xml)
    list(APPEND test_list runner_bm_case_test)
    add_executable(runner_bm runner_bm.cc)
    target_link_libraries(runner_bm runner_bm_case
        benchmark  gtest ${BM_LIBS})



    add_library(engine_bm_case STATIC engine_bm_case.cc)
    add_executable(engine_bm_case_test engine_bm_case_test.cc)
    target_link_libraries(engine_bm_case_test engine_bm_case  fesql_dbms fesql_tablet fesql_flags
        benchmark gtest ${BM_LIBS})

    add_test(engine_bm_case_test engine_bm_case_test --gtest_output=xml:${CMAKE_BINARY_DIR}/engine_bm_case_test.xml)
    list(APPEND test_list engine_bm_case_test)

    add_executable(engine_bm engine_bm.cc)
    target_link_libraries(engine_bm engine_bm_case 
        benchmark gtest ${BM_LIBS})

        #add_executable(memsql_client_bm memsql_client_bm.cc)
        #target_link_libraries(memsql_client_bm ${vm_deps_libs} benchmark mysqlclient ${llvm_libs} ${COMMON_LIBS} brpc protobuf ssl crypto gtest ${OS_LIB})


    add_library(fesql_client_bm_case STATIC fesql_client_bm_case.cc)

    add_executable(fesql_client_bm_case_test fesql_client_bm_case_test.cc)
    target_link_libraries(fesql_client_bm_case_test fesql_client_bm_case
        gtest benchmark ${BM_LIBS})

    add_test(fesql_client_bm_case_test fesql_client_bm_case_test --gtest_output=xml:${CMAKE_BINARY_DIR}/fesql_client_bm_case_test.xml)
    list(APPEND test_list fesql_client_bm_case_test)

    add_executable(fesql_client_batch_run_bm fesql_client_batch_run_bm.cc)
    target_link_libraries(fesql_client_batch_run_bm fesql_client_bm_case
        gtest  benchmark ${BM_LIBS})

endif ()
