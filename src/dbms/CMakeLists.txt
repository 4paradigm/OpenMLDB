include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)

add_library(fesql_dbms STATIC dbms_server_impl.cc)
add_dependencies(fesql_dbms fesql_proto)

