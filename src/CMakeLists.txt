include_directories(${INCLUDE_DIRECTORIES} ${PROJECT_SOURCE_DIR}/src)

add_library(tablet STATIC proto/tablet.pb.cc tablet/tablet_impl.cc flags.cc)
add_library(storage STATIC storage/table.cc storage/segment.cc)

add_executable(skiplist_test base/skiplist_test.cc)
target_link_libraries(skiplist_test gtest pthread tcmalloc_minimal)

add_executable(segment_test  storage/segment.cc storage/segment_test.cc)
target_link_libraries(segment_test gtest pthread common tcmalloc_minimal)

add_executable(table_test  storage/table.cc storage/table_test.cc storage/segment.cc)
target_link_libraries(table_test gtest pthread common tcmalloc_minimal)

add_executable(kv_iterator_test  base/kv_iterator_test.cc proto/tablet.pb.cc)
target_link_libraries(kv_iterator_test gtest pthread common protobuf)

add_executable(tablet_impl_test  tablet/tablet_impl_test.cc)
target_link_libraries(tablet_impl_test   gtest pthread tablet gflags storage common sofa-pbrpc  protobuf z snappy tcmalloc_minimal)

add_executable(codec_bench_test  base/codec_bench_test.cc proto/kv_pair.pb.cc)
target_link_libraries(codec_bench_test   gtest pthread storage tablet gflags storage common protobuf tcmalloc_minimal)


add_executable(rtidb cmd/rtidb.cc client/tablet_client.cc)
target_link_libraries(rtidb gflags pthread common sofa-pbrpc protobuf z snappy tablet storage tcmalloc_minimal)

