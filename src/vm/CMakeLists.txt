include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_vm STATIC jit.cc jit_wrapper.cc runner.cc sql_compiler.cc engine.cc core_api.cc mem_catalog.cc catalog_wrapper.cc schemas_context.cc transform.cc physical_op.cc)
target_link_libraries(fesql_vm fesql_base ${LLVM_LIBS})

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_library(fesql_csv_catalog STATIC csv_catalog.cc csv_table_iterator.cc
        csv_window_iterator.cc)
endif()
add_library(fesql_simple_catalog STATIC simple_catalog.cc)

if (TESTING_ENABLE)
set(vm_deps_libs fesql_case ${yaml_libs} boost_filesystem fesql_dbms fesql_tablet  ${VM_LIBS} fesql_flags ${LLVM_LIBS} ${BRPC_LIBS} ${OS_LIB})


add_executable(physical_op_test physical_op_test.cc)
target_link_libraries(physical_op_test gtest ${vm_deps_libs})
add_test(physical_op_test physical_op_test --gtest_output=xml:${CMAKE_BINARY_DIR}/physical_op_test.xml)
list(APPEND physical_op_test physical_op_test)

add_executable(simple_catalog_test simple_catalog_test.cc)
target_link_libraries(simple_catalog_test fesql_simple_catalog gtest ${vm_deps_libs})
add_test(simple_catalog_test simple_catalog_test --gtest_output=xml:${CMAKE_BINARY_DIR}/simple_catalog_test.xml)
list(APPEND test_list simple_catalog_test)

add_executable(schemas_context_test schemas_context_test.cc)
target_link_libraries(schemas_context_test gtest ${vm_deps_libs})
add_test(schemas_context_test schemas_context_test --gtest_output=xml:${CMAKE_BINARY_DIR}/schemas_context_test.xml)
list(APPEND test_list schemas_context_test)

add_executable(mem_catalog_test mem_catalog_test.cc)
target_link_libraries(mem_catalog_test gtest ${vm_deps_libs})
add_test(mem_catalog_test mem_catalog_test --gtest_output=xml:${CMAKE_BINARY_DIR}/mem_catalog_test.xml)
list(APPEND test_list mem_catalog_test)

add_executable(window_test window_test.cc)
target_link_libraries(window_test gtest ${vm_deps_libs})
add_test(window_test mem_catalog_test --gtest_output=xml:${CMAKE_BINARY_DIR}/window_test.xml)
list(APPEND test_list window_test)

add_executable(transform_test transform_test.cc)
target_link_libraries(transform_test gtest fesql_udf fesql_simple_catalog ${vm_deps_libs})
add_test(transform_test transform_test --gtest_output=xml:${CMAKE_BINARY_DIR}/transform_test.xml)
list(APPEND transform_test transform_test)

add_executable(transform_request_mode_test transform_request_mode_test.cc)
target_link_libraries(transform_request_mode_test gtest fesql_udf ${vm_deps_libs})
add_test(transform_request_mode_test transform_request_mode_test --gtest_output=xml:${CMAKE_BINARY_DIR}/transform_request_mode_test.xml)
list(APPEND transform_request_mode_test transform_request_mode_test)

add_executable(logical_graph_test logical_graph_test.cc)
target_link_libraries(logical_graph_test gtest ${vm_deps_libs})
add_test(logical_graph_test logical_graph_test --gtest_output=xml:${CMAKE_BINARY_DIR}/logical_graph_test.xml)
list(APPEND logical_graph_test logical_graph_test)

#add_executable(csv_catalog_test csv_catalog_test.cc)
#target_link_libraries(csv_catalog_test  fesql_csv_catalog ${vm_deps_libs} ${llvm_libs} ${g_libs} ${arrow_libs} ${COMMON_LIBS} protobuf gtest pthread)
#add_test(csv_catalog_test csv_catalog_test --gtest_output=xml:${CMAKE_BINARY_DIR}/csv_catalog_test.xml)
#list(APPEND test_list csv_catalog_test)


add_executable(jit_test jit_test.cc)
target_link_libraries(jit_test gtest ${vm_deps_libs})
add_test(jit_test jit_test --gtest_output=xml:${CMAKE_BINARY_DIR}/jit_test.xml)
list(APPEND test_list jit_test)

add_executable(jit_wrapper_test jit_wrapper_test.cc)
target_link_libraries(jit_wrapper_test fesql_simple_catalog gtest ${vm_deps_libs})
add_test(jit_wrapper_test jit_wrapper_test --gtest_output=xml:${CMAKE_BINARY_DIR}/jit_wrapper_test.xml)
list(APPEND test_list jit_wrapper_test)

add_executable(runner_test runner_test.cc)
target_link_libraries(runner_test gtest ${vm_deps_libs})
add_test(sql_compiler_test runner_test
        --gtest_output=xml:${CMAKE_BINARY_DIR}/runner_test.xml)
list(APPEND test_list runner_test)

add_executable(sql_compiler_test sql_compiler_test.cc)
target_link_libraries(sql_compiler_test fesql_simple_catalog gtest ${vm_deps_libs})
add_test(sql_compiler_test sql_compiler_test
        --gtest_output=xml:${CMAKE_BINARY_DIR}/sql_compiler_test.xml)
list(APPEND test_list sql_compiler_test)


add_executable(engine_test engine_test.cc)
target_link_libraries(engine_test gtest ${vm_deps_libs})
add_test(engine_test engine_test
        --gtest_output=xml:${CMAKE_BINARY_DIR}/engine_test.xml)
list(APPEND test_list engine_test)

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_executable(csv_db csv_db.cc)
    target_link_libraries(csv_db fesql_csv_catalog gtest ${ARROW_LIBS} ${vm_deps_libs})
endif()

endif()
