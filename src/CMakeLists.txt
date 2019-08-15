include_directories(${INCLUDE_DIRECTORIES} ${PROJECT_SOURCE_DIR}/src)

if (TCMALLOC_ENABLE)
    if (CMAKE_BUILD_TYPE MATCHES Release)
        set(tc_lib tcmalloc_minimal)
    else()
        set(tc_lib unwind tcmalloc)
    endif()
endif()

add_library(tablet STATIC proto/tablet.pb.cc proto/common.pb.cc tablet/tablet_impl.cc flags.cc tablet/tablet_metric.cc)
add_library(nameserver STATIC proto/name_server.pb.cc nameserver/name_server_impl.cc flags.cc)
add_library(storage STATIC storage/mem_table.cc storage/disk_table.cc storage/segment.cc storage/ticket.cc storage/snapshot.cc)
target_link_libraries(storage rocksdb z)
add_library(client STATIC client/tablet_client.cc client/ns_client.cc)
add_library(log STATIC log/crc32c.cc 
                       log/sequential_file.cc 
                       log/log_writer.cc 
                       log/log_reader.cc
                       log/writable_file.cc flags.cc)
add_library(base STATIC base/status.cc base/RingQueue.h)
add_library(zk_client STATIC zk/zk_client.cc zk/dist_lock.cc)

add_library(replica STATIC replica/replicate_node.cc
                           replica/log_replicator.cc)

add_library(rtidb_py SHARED client/tablet_client.cc client/tablet_client_wrapper.cc proto/tablet.pb.cc proto/common.pb.cc)
target_link_libraries(rtidb_py pthread common brpc gflags rt ssl crypto dl leveldb protobuf z snappy)

add_executable(strings_test base/strings_test.cc)
target_link_libraries(strings_test gtest pthread)

add_executable(slice_test base/slice_test.cc)
target_link_libraries(slice_test gtest pthread)


add_executable(zk_test zk/zk_client_test.cc)
target_link_libraries(zk_test gtest zk_client zookeeper_mt pthread common)

add_executable(dist_lock_test zk/dist_lock_test.cc)
target_link_libraries(dist_lock_test gtest zk_client zookeeper_mt pthread common)

add_executable(replica_test replica/log_replicator_test.cc flags.cc base/status.cc)
target_link_libraries(replica_test gtest gflags replica log tablet storage base pthread brpc rt ssl crypto dl leveldb protobuf z snappy common)

add_executable(skiplist_test base/skiplist_test.cc)
target_link_libraries(skiplist_test gtest pthread)

add_executable(flat_array_test base/flat_array_test.cc)
target_link_libraries(flat_array_test gtest pthread)

add_executable(schema_codec_test base/schema_codec_test.cc proto/name_server.pb.cc proto/common.pb.cc)
target_link_libraries(schema_codec_test gtest pthread protobuf)

add_executable(count_down_latch_test base/count_down_latch_test.cc)
target_link_libraries(count_down_latch_test common gtest pthread)

add_executable(segment_test storage/segment_test.cc flags.cc proto/tablet.pb.cc proto/common.pb.cc)
target_link_libraries(segment_test gtest gflags storage rt protobuf pthread common tcmalloc_minimal)

add_executable(table_test  storage/table_test.cc proto/tablet.pb.cc proto/common.pb.cc flags.cc)
target_link_libraries(table_test gtest gflags pthread storage rt common protobuf tcmalloc_minimal)

add_executable(disk_table_test  storage/disk_table_test.cc proto/tablet.pb.cc proto/common.pb.cc flags.cc)
target_link_libraries(disk_table_test gtest gflags pthread storage rt common protobuf tcmalloc_minimal z)

add_executable(snapshot_test  storage/snapshot_test.cc proto/tablet.pb.cc proto/common.pb.cc flags.cc base/status.cc)
target_link_libraries(snapshot_test storage log tablet base common brpc rt ssl crypto dl leveldb protobuf gtest gflags pthread z snappy)

add_executable(table_mem_test  storage/table_mem_test.cc)
target_link_libraries(table_mem_test gtest pthread storage rt zookeeper_mt common zk_client unwind tcmalloc)

add_executable(kv_iterator_test  base/kv_iterator_test.cc proto/tablet.pb.cc proto/common.pb.cc)
target_link_libraries(kv_iterator_test gtest pthread common protobuf)

add_executable(tablet_impl_test  tablet/tablet_impl_test.cc base/status.cc)
target_link_libraries(tablet_impl_test  gtest pthread  tablet gflags storage replica log common brpc rt ssl crypto dl leveldb rt protobuf z snappy zk_client  zookeeper_mt tcmalloc_minimal)

add_executable(tablet_func_test  tablet/tablet_impl_func_test.cc base/status.cc)
target_link_libraries(tablet_func_test  gtest pthread  tablet gflags storage replica log common brpc rt ssl crypto dl leveldb rt protobuf z snappy zk_client  zookeeper_mt tcmalloc_minimal)

add_executable(tablet_impl_keep_alive_test tablet/tablet_impl_keep_alive_test.cc base/status.cc)
target_link_libraries(tablet_impl_keep_alive_test   gtest pthread tablet gflags storage replica log common brpc rt ssl crypto dl leveldb protobuf z snappy zk_client zookeeper_mt tcmalloc_minimal)

add_executable(tablet_impl_mem_test  tablet/tablet_impl_mem_test.cc base/status.cc)
target_link_libraries(tablet_impl_mem_test   gtest pthread tablet gflags storage replica log common brpc rt ssl crypto dl leveldb protobuf z snappy zk_client zookeeper_mt unwind tcmalloc)

add_executable(codec_bench_test  base/codec_bench_test.cc proto/common.pb.cc)
target_link_libraries(codec_bench_test gtest pthread storage tablet gflags common rt protobuf tcmalloc_minimal)

add_executable(codec_test  base/codec_test.cc proto/common.pb.cc)
target_link_libraries(codec_test gtest pthread storage tablet gflags rt common protobuf tcmalloc_minimal)

add_executable(log_test  log/log_test.cc)
target_link_libraries(log_test gtest pthread storage tablet base log gflags common rt protobuf tcmalloc_minimal)

add_executable(snapshot_replica_test  replica/snapshot_replica_test.cc client/tablet_client.cc base/status.cc flags.cc)
target_link_libraries(snapshot_replica_test gtest tablet storage replica log zk_client common brpc  gflags rt ssl crypto dl protobuf z leveldb pthread snappy  zookeeper_mt tcmalloc_minimal)

add_executable(binlog_test  replica/binlog_test.cc client/tablet_client.cc base/status.cc flags.cc)
target_link_libraries(binlog_test gtest gflags tablet zk_client zookeeper_mt storage replica log common brpc rt ssl crypto dl leveldb pthread protobuf z snappy  tcmalloc_minimal)

add_executable(nameserver_test nameserver/name_server_test.cc client/tablet_client.cc client/ns_client.cc base/status.cc)
target_link_libraries(nameserver_test  gtest pthread tablet nameserver gflags storage replica log common brpc ssl crypto dl leveldb protobuf z snappy rt zk_client zookeeper_mt tcmalloc_minimal)

add_executable(rtidb cmd/rtidb.cc base/status.cc proto/client.pb.cc base/linenoise.cc)

target_link_libraries(rtidb gflags common client  tablet storage replica log nameserver brpc ssl crypto dl leveldb protobuf z snappy gflags pthread rt zk_client zookeeper_mt tcmalloc_minimal)
