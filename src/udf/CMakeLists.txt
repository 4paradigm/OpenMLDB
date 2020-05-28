include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)
add_library(fesql_udf STATIC udf.cc)
target_link_libraries(fesql_udf fesql_vm)

if (TESTING_ENABLE)
add_executable(udf_test udf_test.cc)
target_link_libraries(udf_test gtest ${VM_LIBS} fesql_vm ${LLVM_LIBS} ${BRPC_LIBS} ${OS_LIB})
add_test(udf_test udf_test --gtest_output=xml:${CMAKE_BINARY_DIR}/udf_test.xml)
list(APPEND test_list udf_test)
endif()
