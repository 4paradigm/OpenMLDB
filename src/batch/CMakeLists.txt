include_directories(${INCLUDE_DIRECTORIES}
            ${PROJECT_SOURCE_DIR}/src
                    ${PROJECT_BINARY_DIR}/src)
add_library(fesql_batch STATIC batch_catalog.cc)

set(vm_deps_libs fesql_tablet fesql_storage fesql_vm fesql_parser fesql_planner fesql_codegen fesql_proto
    fesql_node fesql_udf fesql_base)

add_executable(batch_catalog_test batch_catalog_test.cc)
target_link_libraries(batch_catalog_test fesql_batch fesql_flags fesql_proto
                     ${vm_deps_libs} ${arrow_libs} protobuf ${g_libs} gtest pthread)
add_test(batch_catalog_test batch_catalog_test --gtest_output=xml:${CMAKE_BINARY_DIR}/batch_catalog_test.xml)
list(APPEND test_list batch_catalog_test)
