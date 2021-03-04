# fesql bin
add_executable(fesql fesql.cc)
target_link_libraries(fesql fesql_core fesql_flags fesql_sdk)

# udf exporter
add_executable(export_udf_info export_udf_info.cc)
target_link_libraries(export_udf_info fesql_core)

# engine execution
add_executable(fesql_run_engine fesql_run_engine.cc)
target_link_libraries(fesql_run_engine fesql_core fesql_flags gtest sqlite3)
