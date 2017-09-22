include_directories(${INCLUDE_DIRECTORIES} ${PROJECT_SOURCE_DIR}/src)

if (TCMALLOC_ENABLE)
    if (CMAKE_BUILD_TYPE MATCHES Release)
        set(tc_lib tcmalloc_minimal)
    else()
        set(tc_lib unwind tcmalloc)
    endif()
endif()

add_library(tablet STATIC proto/tablet.pb.cc tablet/tablet_impl.cc flags.cc tablet/tablet_metric.cc)
add_library(nameserver STATIC proto/name_server.pb.cc nameserver/name_server_impl.cc flags.cc)
add_library(storage STATIC storage/table.cc storage/segment.cc storage/ticket.cc storage/snapshot.cc)
add_library(client STATIC client/tablet_client.cc client/ns_client.cc)
add_library(log STATIC log/crc32c.cc 
                       log/sequential_file.cc 
                       log/log_writer.cc 
                       log/log_reader.cc
                       log/writable_file.cc flags.cc)
add_library(base STATIC base/status.cc)
add_library(zk_client STATIC zk/zk_client.cc zk/dist_lock.cc)

add_library(replica STATIC replica/replicate_node.cc
                           replica/log_replicator.cc)

add_library(rtidb_py SHARED client/tablet_client.cc client/tablet_client_wrapper.cc proto/tablet.pb.cc)
target_link_libraries(rtidb_py pthread common sofa-pbrpc protobuf z snappy)

add_executable(strings_test base/strings_test.cc)
target_link_libraries(strings_test gtest pthread)

add_executable(zk_test zk/zk_client_test.cc)
target_link_libraries(zk_test gtest zk_client zookeeper_mt pthread common)

add_executable(dist_lock_test zk/dist_lock_test.cc)
target_link_libraries(dist_lock_test gtest zk_client zookeeper_mt pthread common)

add_executable(replica_test replica/log_replicator_test.cc flags.cc base/status.cc)
target_link_libraries(replica_test gtest gflags replica log tablet storage base pthread sofa-pbrpc protobuf z snappy common)

add_executable(skiplist_test base/skiplist_test.cc)
target_link_libraries(skiplist_test gtest pthread unwind tcmalloc)

add_executable(segment_test storage/segment_test.cc flags.cc)
target_link_libraries(segment_test gtest gflags storage pthread common unwind tcmalloc)

add_executable(table_test  storage/table_test.cc flags.cc)
target_link_libraries(table_test gtest gflags pthread storage common unwind tcmalloc)

add_executable(snapshot_test  storage/snapshot_test.cc proto/tablet.pb.cc flags.cc)
target_link_libraries(snapshot_test gtest log base pthread storage tablet protobuf common gflags unwind tcmalloc)

add_executable(table_mem_test  storage/table_mem_test.cc)
target_link_libraries(table_mem_test gtest pthread storage zookeeper_mt common zk_client unwind tcmalloc)

add_executable(kv_iterator_test  base/kv_iterator_test.cc proto/tablet.pb.cc)
target_link_libraries(kv_iterator_test gtest pthread common protobuf)

add_executable(tablet_impl_test  tablet/tablet_impl_test.cc base/status.cc)
target_link_libraries(tablet_impl_test  gtest pthread  tablet gflags storage replica log common sofa-pbrpc  protobuf z snappy zk_client  zookeeper_mt unwind tcmalloc)

add_executable(tablet_impl_keep_alive_test tablet/tablet_impl_keep_alive_test.cc base/status.cc)
target_link_libraries(tablet_impl_keep_alive_test   gtest pthread tablet gflags storage replica log common sofa-pbrpc  protobuf z snappy zk_client zookeeper_mt unwind tcmalloc)

add_executable(tablet_impl_mem_test  tablet/tablet_impl_mem_test.cc base/status.cc)
target_link_libraries(tablet_impl_mem_test   gtest pthread tablet gflags storage replica log common sofa-pbrpc  protobuf z snappy zk_client zookeeper_mt unwind tcmalloc)

add_executable(codec_bench_test  base/codec_bench_test.cc proto/kv_pair.pb.cc)
target_link_libraries(codec_bench_test   gtest pthread storage tablet gflags storage common protobuf unwind tcmalloc)

add_executable(log_test  log/log_test.cc)
target_link_libraries(log_test   gtest pthread storage tablet base log gflags storage common protobuf tcmalloc_minimal)

add_executable(snapshot_replica_test  replica/snapshot_replica_test.cc client/tablet_client.cc base/status.cc flags.cc)
target_link_libraries(snapshot_replica_test gtest gflags pthread common sofa-pbrpc protobuf z snappy tablet storage replica log zk_client zookeeper_mt tcmalloc_minimal)

add_executable(binlog_test  replica/binlog_test.cc client/tablet_client.cc base/status.cc flags.cc)
target_link_libraries(binlog_test gtest gflags pthread common sofa-pbrpc protobuf z snappy tablet zk_client zookeeper_mt storage replica log tcmalloc_minimal)

add_executable(nameserver_test nameserver/name_server_test.cc client/tablet_client.cc base/status.cc)
target_link_libraries(nameserver_test  gtest pthread tablet nameserver gflags storage replica log common sofa-pbrpc  protobuf z snappy zk_client zookeeper_mt unwind tcmalloc)

add_executable(rtidb cmd/rtidb.cc base/status.cc)
target_link_libraries(rtidb gflags pthread common client sofa-pbrpc protobuf z snappy tablet storage replica log nameserver zk_client zookeeper_mt tcmalloc_minimal)

