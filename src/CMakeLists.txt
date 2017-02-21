INCLUDE_DIRECTORIES(${INCLUDE_DIRECTORIES} 
  ${PROJECT_SOURCE_DIR}/src
  ${PROJECT_SOURCE_DIR}/src/proto)
set(util_src util/hash.cc util/status.cc util/filter_policy.cc util/coding.cc util/comparator.cc util/arena.cc)
set(storage_src storage/iterator.cc storage/memtable.cc storage/dbformat.cc)
set(dbms_src dbms/rtidb_tablet_server_impl.cc)
set(rtidb_src rtidb_main.cc)
set(tablet_src proto/rtidb_tablet_server.pb.cc)
ADD_EXECUTABLE(rtidb ${rtidb_src} ${storage_src} ${util_src} ${dbms_src} ${tablet_src})
TARGET_LINK_LIBRARIES(rtidb common pthread protobuf sofa-pbrpc z gflags snappy)

