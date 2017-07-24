include_directories(${INCLUDE_DIRECTORIES} ${PROJECT_SOURCE_DIR}/src)

if (TCMALLOC_ENABLE)
    if (CMAKE_BUILD_TYPE MATCHES Release)
        set(tc_lib tcmalloc_minimal)
    else()
        set(tc_lib unwind tcmalloc)
    endif()
endif()

add_library(tablet STATIC proto/tablet.pb.cc tablet/tablet_impl.cc flags.cc tablet/tablet_metric.cc)
add_library(storage STATIC storage/table.cc storage/segment.cc storage/ticket.cc)

add_library(log STATIC log/crc32c.cc 
                       log/sequential_file.cc 
                       log/log_writer.cc 
                       log/log_reader.cc
                       log/writable_file.cc)
add_library(base STATIC base/status.cc)

add_library(replica STATIC replica/log_replicator.cc)

add_library(rtidb_py SHARED client/tablet_client.cc client/tablet_client_wrapper.cc proto/tablet.pb.cc)
target_link_libraries(rtidb_py pthread common sofa-pbrpc protobuf z snappy)

add_executable(replica_test replica/log_replicator_test.cc flags.cc base/status.cc)
target_link_libraries(replica_test gtest gflags replica log tablet pthread sofa-pbrpc protobuf z snappy common leveldb)

add_executable(skiplist_test base/skiplist_test.cc)
target_link_libraries(skiplist_test gtest pthread unwind tcmalloc)

add_executable(segment_test storage/segment_test.cc)
target_link_libraries(segment_test gtest storage pthread common unwind tcmalloc)

add_executable(table_test  storage/table_test.cc )
target_link_libraries(table_test gtest pthread storage common unwind tcmalloc)

add_executable(table_mem_test  storage/table_mem_test.cc)
target_link_libraries(table_mem_test gtest pthread storage common unwind tcmalloc)

add_executable(kv_iterator_test  base/kv_iterator_test.cc proto/tablet.pb.cc)
target_link_libraries(kv_iterator_test gtest pthread common protobuf)

add_executable(tablet_impl_test  tablet/tablet_impl_test.cc base/status.cc)
target_link_libraries(tablet_impl_test   gtest pthread tablet leveldb gflags storage replica log common sofa-pbrpc  protobuf z snappy leveldb unwind tcmalloc)

add_executable(tablet_impl_mem_test  tablet/tablet_impl_mem_test.cc base/status.cc)
target_link_libraries(tablet_impl_mem_test   gtest pthread tablet leveldb gflags storage replica log common sofa-pbrpc  protobuf z snappy unwind leveldb tcmalloc)


add_executable(codec_bench_test  base/codec_bench_test.cc proto/kv_pair.pb.cc)
target_link_libraries(codec_bench_test   gtest pthread storage tablet gflags storage common protobuf unwind tcmalloc)

add_executable(log_test  log/log_test.cc)
target_link_libraries(log_test   gtest pthread storage tablet base log gflags storage common protobuf tcmalloc_minimal)

add_executable(rtidb cmd/rtidb.cc client/tablet_client.cc base/status.cc)
target_link_libraries(rtidb gflags pthread common sofa-pbrpc protobuf z snappy tablet storage replica log leveldb tcmalloc_minimal)

