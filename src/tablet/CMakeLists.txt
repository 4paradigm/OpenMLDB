include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)
add_library(fesql_tablet STATIC tablet_server_impl.cc tablet_internal_sdk.cc)
add_dependencies(fesql_tablet run_gen_proto)
