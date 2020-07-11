add_subdirectory(proto)
add_subdirectory(base)
add_subdirectory(udf)
add_subdirectory(node)
add_subdirectory(parser)
add_subdirectory(plan)
add_subdirectory(codegen)
add_subdirectory(storage)
add_subdirectory(dbms)
add_subdirectory(sdk)
add_subdirectory(vm)
add_subdirectory(tablet)
add_subdirectory(bm)
add_subdirectory(batch)
add_subdirectory(codec)
add_subdirectory(case)

include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_flags STATIC flags.cc)
add_executable(fesql cmd/fesql.cc)
target_link_libraries(fesql fesql_dbms fesql_tablet fesql_sdk 
    fesql_flags 
    ${VM_LIBS} boost_filesystem ${LLVM_LIBS} ${BRPC_LIBS} tcmalloc ${OS_LIB})
