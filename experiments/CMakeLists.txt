include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_executable(bitcode_test bitcode/bitcode_test.cc)
target_link_libraries(bitcode_test ${llvm_libs} ${COMMON_LIBS} gtest pthread)

add_executable(sql_parquet_test parquet/sql_parquet_demo.cc)
target_link_libraries(sql_parquet_test ${arrow_libs} ${COMMON_LIBS} gtest pthread)

add_executable(boost_test boost/boost_test.cc)
target_link_libraries(boost_test ${boost_libs} ${COMMON_LIBS} gtest pthread)


add_executable(io_buf_test iobuf/test_iobuf.cc)
target_link_libraries(io_buf_test brpc ${COMMON_LIBS} leveldb protobuf z snappy ssl crypto dl gtest pthread)

