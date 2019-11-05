add_subdirectory(base)
add_subdirectory(node)
add_subdirectory(parser)
add_subdirectory(analyser)
add_subdirectory(plan)
add_subdirectory(proto)
add_subdirectory(codegen)
add_subdirectory(storage)
add_subdirectory(dbms)
add_subdirectory(sdk)

include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)

add_executable(fesql cmd/fesql.cc flags.cc)
target_link_libraries(fesql fesql_dbms fesql_sdk fesql_base fesql_proto fesql_parser fesql_node fesql_codegen gflags brpc glog lzma leveldb protobuf z snappy ssl crypto dl pthread)
#target_link_libraries(fesql fesql_dbms fesql_sdk fesql_base fesql_proto fesql_parser fesql_node fesql_codegen gflags brpc glog unwind lzma leveldb protobuf z snappy ssl crypto dl pthread)



