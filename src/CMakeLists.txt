include_directories(${INCLUDE_DIRECTORIES} ${PROJECT_SOURCE_DIR}/src)

if (TCMALLOC_ENABLE)
    if (CMAKE_BUILD_TYPE MATCHES Release)
        set(tc_lib tcmalloc_minimal)
    else()
        set(tc_lib unwind tcmalloc)
    endif()
endif()

add_library(tablet STATIC proto/tablet.pb.cc tablet/table_ha.cc tablet/tablet_impl.cc flags.cc tablet/tablet_metric.cc)
add_library(storage STATIC storage/table.cc storage/segment.cc)

add_library(log STATIC log/coding.cc 
                       log/crc32c.cc 
                       log/sequential_file.cc 
                       log/log_writer.cc 
                       log/log_reader.cc
                       log/writable_file.cc)

add_library(replica STATIC replica/log_replicator.cc)

add_library(rtidb_py SHARED client/tablet_client.cc client/tablet_client_wrapper.cc proto/tablet.pb.cc)
target_link_libraries(rtidb_py pthread common sofa-pbrpc protobuf z snappy)

add_executable(replica_test replica/log_replicator_test.cc)
target_link_libraries(replica_test pthread gtest common replica)

add_executable(skiplist_test base/skiplist_test.cc)
target_link_libraries(skiplist_test gtest pthread ${tc_lib})

add_executable(segment_test  storage/segment.cc storage/segment_test.cc)
target_link_libraries(segment_test gtest pthread common ${tc_lib})

add_executable(table_test  storage/table.cc storage/table_test.cc storage/segment.cc)
target_link_libraries(table_test gtest pthread common ${tc_lib})

add_executable(table_mem_test  storage/table.cc storage/table_mem_test.cc storage/segment.cc)
target_link_libraries(table_mem_test gtest pthread common ${tc_lib})

add_executable(kv_iterator_test  base/kv_iterator_test.cc proto/tablet.pb.cc)
target_link_libraries(kv_iterator_test gtest pthread common protobuf)

add_executable(tablet_impl_test  tablet/tablet_impl_test.cc)
target_link_libraries(tablet_impl_test   gtest pthread tablet leveldb gflags storage common sofa-pbrpc  protobuf z snappy ${tc_lib})

add_executable(tablet_impl_mem_test  tablet/tablet_impl_mem_test.cc)
target_link_libraries(tablet_impl_mem_test   gtest pthread tablet leveldb gflags storage common sofa-pbrpc  protobuf z snappy ${tc_lib})


add_executable(table_ha_test  tablet/table_ha_test.cc)
target_link_libraries(table_ha_test   gtest pthread tablet leveldb gflags storage common sofa-pbrpc  protobuf z snappy ${tc_lib})


add_executable(codec_bench_test  base/codec_bench_test.cc proto/kv_pair.pb.cc)
target_link_libraries(codec_bench_test   gtest pthread storage tablet gflags storage common protobuf ${tc_lib})


add_executable(rtidb cmd/rtidb.cc client/tablet_client.cc)
target_link_libraries(rtidb gflags pthread common sofa-pbrpc protobuf z snappy tablet storage leveldb ${tc_lib})

