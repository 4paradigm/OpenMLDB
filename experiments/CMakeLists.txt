include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_executable(bitcode_test bitcode/bitcode_test.cc)
target_link_libraries(bitcode_test ${llvm_libs} ${COMMON_LIBS} gtest pthread)

add_executable(sql_parquet_test parquet/sql_parquet_demo.cc)
target_link_libraries(sql_parquet_test ${arrow_libs} ${g_libs} gtest pthread)
