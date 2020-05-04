include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_executable(bitcode_test bitcode/bitcode_test.cc)
target_link_libraries(bitcode_test ${LLVM_LIBS}  gtest pthread)

add_executable(sql_parquet_test parquet/sql_parquet_demo.cc)
target_link_libraries(sql_parquet_test ${ARROW_LIBS} glog gflags  gtest pthread)

add_executable(boost_test boost/boost_test.cc)
target_link_libraries(boost_test ${boost_libs}  gtest pthread)


add_executable(io_buf_test iobuf/test_iobuf.cc)
target_link_libraries(io_buf_test ${BRPC_LIBS} gtest pthread)

