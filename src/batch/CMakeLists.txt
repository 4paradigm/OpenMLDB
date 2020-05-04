include_directories(${INCLUDE_DIRECTORIES}
            ${PROJECT_SOURCE_DIR}/src
                    ${PROJECT_BINARY_DIR}/src)
add_library(fesql_batch STATIC batch_catalog.cc)

set(vm_deps_libs fesql_sdk fesql_dbms fesql_tablet fesql_flags ${ARROW_LIBS} ${VM_LIBS} ${LLVM_LIBS} ${BRPC_LIBS} ${OS_LIB})

add_executable(batch_catalog_test batch_catalog_test.cc)
target_link_libraries(batch_catalog_test gtest 
    fesql_batch ${vm_deps_libs})
add_test(batch_catalog_test batch_catalog_test --gtest_output=xml:${CMAKE_BINARY_DIR}/batch_catalog_test.xml)
list(APPEND test_list batch_catalog_test)
