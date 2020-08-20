# fesql bin
add_library(fesql_flags STATIC ${CMAKE_SOURCE_DIR}/src/flags.cc)
add_executable(fesql fesql.cc)
target_link_libraries(fesql fesql_core fesql_flags fesql_sdk
    tcmalloc)

# udf exporter
add_executable(export_udf_info export_udf_info.cc)
target_link_libraries(export_udf_info fesql_core)