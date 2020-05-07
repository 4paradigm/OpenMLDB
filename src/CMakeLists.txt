include_directories(${INCLUDE_DIRECTORIES} ${PROJECT_SOURCE_DIR}/src)

set(BIN_LIBS client nameserver tablet client blobserver blob_proxy zk_client replica  base beans storage log common rocksdb zookeeper_mt tcmalloc_minimal ${BRPC_LIBS})

add_library(tablet STATIC proto/tablet.pb.cc proto/common.pb.cc proto/type.pb.cc tablet/tablet_impl.cc flags.cc tablet/file_receiver.cc tablet/file_sender.cc)
add_library(blobserver STATIC proto/blob_server.pb.cc storage/object_store.cc proto/type.pb.cc blobserver/blobserver_impl.cc flags.cc)
add_library(nameserver STATIC proto/name_server.pb.cc nameserver/name_server_impl.cc flags.cc)
add_library(beans storage/beans/bitcask.c storage/beans/clock_gettime_stub.c storage/beans/codec.c storage/beans/common.c storage/beans/crc32.c storage/beans/diskmgr.c storage/beans/hint.c storage/beans/hstore.c storage/beans/htree.c storage/beans/mfile.c storage/beans/quicklz.c storage/beans/record.c)
add_library(storage STATIC storage/object_store.cc storage/mem_table.cc storage/disk_table.cc storage/relational_table.cc storage/segment.cc storage/ticket.cc storage/mem_table_snapshot.cc storage/disk_table_snapshot.cc storage/snapshot.cc storage/table.cc storage/binlog.cc codec/codec.cc storage/schema.cc)
add_library(client STATIC client/tablet_client.cc client/ns_client.cc client/bs_client.cc)
add_library(log STATIC log/crc32c.cc 
                       log/sequential_file.cc 
                       log/log_writer.cc 
                       log/log_reader.cc
                       log/writable_file.cc flags.cc)

add_library(blob_proxy STATIC proto/blob_proxy.pb.cc blob_proxy/blob_proxy_impl.cc flags.cc client/client.cc)

add_library(base STATIC base/status.cc base/ringqueue.h)
add_library(zk_client STATIC zk/zk_client.cc zk/dist_lock.cc)

add_library(replica STATIC replica/replicate_node.cc
                           replica/log_replicator.cc)

add_executable(strings_test base/strings_test.cc)
target_link_libraries(strings_test gtest pthread)

add_executable(file_util_test base/file_util_test.cc)
target_link_libraries(file_util_test gtest pthread common)

add_executable(slice_test base/slice_test.cc)
target_link_libraries(slice_test gtest pthread)

add_executable(zk_test zk/zk_client_test.cc)
target_link_libraries(zk_test gtest zk_client zookeeper_mt pthread common)

add_executable(dist_lock_test zk/dist_lock_test.cc)
target_link_libraries(dist_lock_test gtest zk_client zookeeper_mt pthread common)

add_executable(replica_test replica/log_replicator_test.cc flags.cc base/status.cc)
target_link_libraries(replica_test gtest ${BIN_LIBS})

add_executable(skiplist_test base/skiplist_test.cc)
target_link_libraries(skiplist_test gtest pthread)

add_executable(flat_array_test codec/flat_array_test.cc)
target_link_libraries(flat_array_test gtest pthread)

add_executable(schema_codec_test codec/schema_codec_test.cc proto/tablet.pb.cc proto/name_server.pb.cc proto/common.pb.cc proto/type.pb.cc)
target_link_libraries(schema_codec_test gtest pthread protobuf)

add_executable(count_down_latch_test base/count_down_latch_test.cc)
target_link_libraries(count_down_latch_test common gtest pthread)

add_executable(ringqueue_test base/ringqueue_test.cc)
target_link_libraries(ringqueue_test gtest pthread)

add_executable(segment_test storage/segment_test.cc flags.cc)
target_link_libraries(segment_test gtest ${BIN_LIBS})

add_executable(table_test  storage/table_test.cc proto/tablet.pb.cc proto/common.pb.cc proto/type.pb.cc flags.cc)
target_link_libraries(table_test gtest ${BIN_LIBS})

add_executable(disk_table_test  storage/disk_table_test.cc proto/tablet.pb.cc proto/common.pb.cc proto/type.pb.cc flags.cc)
target_link_libraries(disk_table_test gtest ${BIN_LIBS})

add_executable(snapshot_test  storage/snapshot_test.cc proto/tablet.pb.cc proto/common.pb.cc proto/type.pb.cc flags.cc base/status.cc)
target_link_libraries(snapshot_test gtest ${BIN_LIBS})

add_executable(table_mem_test  storage/table_mem_test.cc)
target_link_libraries(table_mem_test gtest ${BIN_LIBS})

add_executable(kv_iterator_test  base/kv_iterator_test.cc proto/tablet.pb.cc proto/common.pb.cc proto/type.pb.cc)
target_link_libraries(kv_iterator_test gtest ${BIN_LIBS})

add_executable(tablet_impl_test  tablet/tablet_impl_test.cc base/status.cc)
target_link_libraries(tablet_impl_test  gtest ${BIN_LIBS})

add_executable(tablet_func_test  tablet/tablet_impl_func_test.cc base/status.cc)
target_link_libraries(tablet_func_test  gtest ${BIN_LIBS})

add_executable(tablet_impl_keep_alive_test tablet/tablet_impl_keep_alive_test.cc base/status.cc)
target_link_libraries(tablet_impl_keep_alive_test  gtest ${BIN_LIBS})

add_executable(tablet_impl_mem_test  tablet/tablet_impl_mem_test.cc base/status.cc)
target_link_libraries(tablet_impl_mem_test   gtest  tcmalloc ${BIN_LIBS})

add_executable(tablet_impl_multi_path_test  tablet/tablet_impl_multi_path_test.cc base/status.cc)
target_link_libraries(tablet_impl_multi_path_test  gtest ${BIN_LIBS})

add_executable(tablet_impl_projection_test  tablet/tablet_impl_projection_test.cc base/status.cc)
target_link_libraries(tablet_impl_projection_test   gtest ${BIN_LIBS})

add_executable(codec_bench_test  codec/codec_bench_test.cc proto/common.pb.cc proto/type.pb.cc)
target_link_libraries(codec_bench_test gtest ${BIN_LIBS})

add_executable(codec_test  codec/codec_test.cc proto/common.pb.cc proto/type.pb.cc codec/codec.cc)
target_link_libraries(codec_test gtest ${BIN_LIBS})

add_executable(project_codec_test  codec/codec_project_test.cc proto/common.pb.cc proto/type.pb.cc codec/codec.cc)
target_link_libraries(project_codec_test gtest ${BIN_LIBS})

add_executable(log_test  log/log_test.cc)
target_link_libraries(log_test gtest ${BIN_LIBS})

add_executable(snapshot_replica_test  replica/snapshot_replica_test.cc client/tablet_client.cc base/status.cc flags.cc)
target_link_libraries(snapshot_replica_test gtest ${BIN_LIBS})

add_executable(binlog_test  replica/binlog_test.cc client/tablet_client.cc base/status.cc flags.cc)
target_link_libraries(binlog_test gtest ${BIN_LIBS})

add_executable(nameserver_test nameserver/name_server_test.cc client/tablet_client.cc client/ns_client.cc base/status.cc)
target_link_libraries(nameserver_test  gtest  ${BIN_LIBS})

add_executable(blobserver_impl_test blobserver/blobserver_impl_test.cc proto/common.pb.cc proto/tablet.pb.cc flags.cc)
target_link_libraries(blobserver_impl_test gtest ${BIN_LIBS})

add_executable(blob_proxy_impl_test blob_proxy/blob_proxy_impl_test.cc proto/common.pb.cc proto/tablet.pb.cc flags.cc)
target_link_libraries(blob_proxy_impl_test gtest ${BIN_LIBS})

add_executable(nameserver_create_remote_test nameserver/name_server_create_remote_test.cc client/tablet_client.cc client/ns_client.cc base/status.cc)
target_link_libraries(nameserver_create_remote_test gtest ${BIN_LIBS})

add_executable(nameserver_object_store_test nameserver/name_server_object_store_test.cc base/status.cc)
target_link_libraries(nameserver_object_store_test gtest ${BIN_LIBS})

add_executable(memcomparable_format_test codec/memcomparable_format_test.cc)
target_link_libraries(memcomparable_format_test gtest pthread)

add_executable(field_codec_test codec/field_codec_test.cc)
target_link_libraries(field_codec_test gtest pthread common)

add_executable(rtidb cmd/rtidb.cc base/status.cc proto/client.pb.cc base/linenoise.cc)

target_link_libraries(rtidb ${BIN_LIBS})
add_subdirectory(client)
