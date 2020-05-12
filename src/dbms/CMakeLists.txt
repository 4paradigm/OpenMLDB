include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_dbms STATIC dbms_server_impl.cc)

if (TESTING_ENABLE)
    add_executable(dbms_server_test dbms_server_impl_test.cc)
    target_link_libraries(dbms_server_test gtest fesql_dbms fesql_tablet fesql_flags
        ${VM_LIBS} ${LLVM_LIBS} ${BRPC_LIBS} ${OS_LIB})
    add_test(dbms_server_test dbms_server_test --gtest_output=xml:${CMAKE_BINARY_DIR}/dbms_server_test.xml)
    list(APPEND test_list dbms_server_test)
endif ()
