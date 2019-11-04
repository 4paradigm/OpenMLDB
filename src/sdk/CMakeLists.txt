include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)

add_library(fesql_sdk STATIC dbms_sdk.cc)
add_dependencies(fesql_sdk fesql_proto)
