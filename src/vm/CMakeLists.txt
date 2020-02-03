include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_catalog STATIC batch_catalog.cc)
add_executable(batch_catalog_test batch_catalog_test.cc)
target_link_libraries(batch_catalog_test fesql_catalog fesql_flags fesql_proto
 ${arrow_libs} protobuf ${g_libs} gtest pthread)
list(APPEND test_list batch_catalog_test)
