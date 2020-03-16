add_subdirectory(proto)
add_subdirectory(base)
add_subdirectory(udf)
add_subdirectory(node)
add_subdirectory(parser)
add_subdirectory(analyser)
add_subdirectory(plan)
add_subdirectory(codegen)
add_subdirectory(storage)
add_subdirectory(dbms)
add_subdirectory(sdk)
add_subdirectory(vm)
add_subdirectory(tablet)
add_subdirectory(bm)
add_subdirectory(batch)

include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_flags STATIC flags.cc)
add_executable(fesql cmd/fesql.cc)
target_link_libraries(fesql fesql_dbms fesql_tablet fesql_sdk fesql_base
    fesql_flags
    fesql_proto fesql_vm fesql_udf fesql_parser fesql_analyser fesql_planner
    fesql_codegen fesql_node  fesql_storage brpc ${llvm_libs} ${COMMON_LIBS} leveldb protobuf z snappy ssl crypto dl pthread)
