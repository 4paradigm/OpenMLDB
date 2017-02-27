INCLUDE_DIRECTORIES(${INCLUDE_DIRECTORIES} 
  ${PROJECT_SOURCE_DIR}/src
  ${PROJECT_SOURCE_DIR}/src/proto)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DSOFA_PBRPC_PROFILING")
set(util_src util/hash.cc util/status.cc util/filter_policy.cc util/coding.cc util/comparator.cc util/arena.cc)
set(storage_src storage/iterator.cc storage/memtable.cc storage/dbformat.cc)
set(dbms_src dbms/rtidb_tablet_server_impl.cc)
set(rtidb_src rtidb_main.cc)
set(tablet_src proto/rtidb_tablet_server.pb.cc)
set(cli_src  cli/rtidb_cli.cc)
ADD_EXECUTABLE(rtidb ${rtidb_src} ${storage_src} ${util_src} ${dbms_src} ${tablet_src})
TARGET_LINK_LIBRARIES(rtidb common pthread sofa-pbrpc protobuf z gflags snappy profiler tcmalloc_and_profiler unwind tcmalloc)

ADD_EXECUTABLE(rtidb_cli ${cli_src} ${tablet_src})
TARGET_LINK_LIBRARIES(rtidb_cli common pthread protobuf sofa-pbrpc z gflags snappy)
