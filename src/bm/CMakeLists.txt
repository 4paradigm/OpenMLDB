include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

set(vm_deps_libs fesql_tablet fesql_vm fesql_udf fesql_vm fesql_parser fesql_planner fesql_codegen fesql_storage fesql_codec fesql_proto
        fesql_node fesql_base)

add_executable(base_bm_test base_bm_test.cc)
target_link_libraries(base_bm_test ${vm_deps_libs} protobuf ${llvm_libs} ${COMMON_LIBS} gflags gtest ${OS_LIB})
add_test(base_bm_test base_bm_test --gtest_output=xml:${CMAKE_BINARY_DIR}/base_bm_test.xml)
list(APPEND test_list base_bm_test)

add_library(storage_bm_case STATIC storage_bm_case.cc)
add_executable(storage_bm_case_test storage_bm_case_test.cc)
target_link_libraries(storage_bm_case_test storage_bm_case ${vm_deps_libs}
        benchmark protobuf ${llvm_libs} ${COMMON_LIBS} tcmalloc profiler gtest ${OS_LIB})
add_test(storage_bm_case_test storage_bm_case_test --gtest_output=xml:${CMAKE_BINARY_DIR}/storage_bm_case_test.xml)
list(APPEND test_list storage_bm_case_test)
add_executable(storage_bm storage_bm.cc)
target_link_libraries(storage_bm storage_bm_case ${vm_deps_libs}
        benchmark protobuf ${llvm_libs} ${COMMON_LIBS} tcmalloc profiler gtest ${OS_LIB})

add_library(udf_bm_case STATIC udf_bm_case.cc)
add_executable(udf_bm_case_test udf_bm_case_test.cc)
target_link_libraries(udf_bm_case_test udf_bm_case ${vm_deps_libs}
        benchmark protobuf ${llvm_libs} ${COMMON_LIBS} tcmalloc profiler gtest ${OS_LIB})
add_test(udf_bm_case_test udf_bm_case_test --gtest_output=xml:${CMAKE_BINARY_DIR}/udf_bm_case_test.xml)
list(APPEND test_list udf_bm_case_test)
add_executable(udf_bm udf_bm.cc)
target_link_libraries(udf_bm udf_bm_case ${vm_deps_libs}
        benchmark protobuf ${llvm_libs} ${COMMON_LIBS} tcmalloc profiler gtest ${OS_LIB})

add_library(runner_bm_case STATIC runner_bm_case.cc)
add_executable(runner_bm_case_test runner_bm_case_test.cc)
target_link_libraries(runner_bm_case_test runner_bm_case ${vm_deps_libs}
        benchmark protobuf ${llvm_libs} ${COMMON_LIBS} tcmalloc profiler gtest ${OS_LIB})

add_test(runner_bm_case_test runner_bm_case_test --gtest_output=xml:${CMAKE_BINARY_DIR}/runner_bm_case_test.xml)
list(APPEND test_list runner_bm_case_test)
add_executable(runner_bm runner_bm.cc)
target_link_libraries(runner_bm runner_bm_case ${vm_deps_libs}
        benchmark protobuf ${llvm_libs} ${COMMON_LIBS} tcmalloc profiler gtest ${OS_LIB})



add_library(engine_bm_case STATIC engine_bm_case.cc)
add_executable(engine_bm_case_test engine_bm_case_test.cc)
target_link_libraries(engine_bm_case_test engine_bm_case ${vm_deps_libs}
        benchmark protobuf ${llvm_libs} ${COMMON_LIBS} tcmalloc profiler gtest ${OS_LIB})

add_test(engine_bm_case_test engine_bm_case_test --gtest_output=xml:${CMAKE_BINARY_DIR}/engine_bm_case_test.xml)
list(APPEND test_list engine_bm_case_test)

add_executable(engine_bm engine_bm.cc)
target_link_libraries(engine_bm engine_bm_case ${vm_deps_libs}
        benchmark protobuf ${llvm_libs} ${COMMON_LIBS} tcmalloc profiler gtest ${OS_LIB})

#add_executable(memsql_client_bm memsql_client_bm.cc)
#target_link_libraries(memsql_client_bm benchmark mysqlclient ${COMMON_LIBS} gtest ${OS_LIB})


add_library(fesql_client_bm_case STATIC fesql_client_bm_case.cc)

add_executable(fesql_client_bm_case_test fesql_client_bm_case_test.cc)
target_link_libraries(fesql_client_bm_case_test fesql_client_bm_case
    fesql_dbms
    fesql_tablet
    fesql_storage fesql_vm fesql_codegen fesql_udf fesql_base
    fesql_flags
    gtest fesql_sdk benchmark ${COMMON_LIBS} ${llvm_libs})

add_test(fesql_client_bm_case_test fesql_client_bm_case_test --gtest_output=xml:${CMAKE_BINARY_DIR}/fesql_client_bm_case_test.xml)
list(APPEND test_list fesql_client_bm_case_test)

add_executable(fesql_client_batch_run_bm fesql_client_batch_run_bm.cc)
target_link_libraries(fesql_client_batch_run_bm fesql_client_bm_case
    fesql_dbms
    fesql_tablet
    fesql_storage fesql_vm fesql_codegen fesql_udf fesql_base
    fesql_flags
    gtest fesql_sdk benchmark ${COMMON_LIBS} ${llvm_libs})

