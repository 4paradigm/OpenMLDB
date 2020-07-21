include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)
add_library(fesql_passes STATIC resolve_udf_def.cc)

if (TESTING_ENABLE)
set(vm_deps_libs
        fesql_case boost_filesystem ${yaml_libs} ${VM_LIBS} fesql_flags ${LLVM_LIBS} ${BRPC_LIBS} ${OS_LIB})

add_executable(resolve_udf_def_test resolve_udf_def_test.cc)
target_link_libraries(resolve_udf_def_test gtest ${vm_deps_libs})
add_test(resolve_udf_def_test resolve_udf_def_test --gtest_output=xml:${CMAKE_BINARY_DIR}/resolve_udf_def_test.xml)
list(APPEND test_list resolve_udf_def_test)

endif()

