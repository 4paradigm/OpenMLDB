INCLUDE_DIRECTORIES(${INCLUDE_DIRECTORIES} 
  ${PROJECT_SOURCE_DIR}/src
  ${PROJECT_SOURCE_DIR}/src/proto)

SET(util_src util/hash.cc util/status.cc util/filter_policy.cc util/coding.cc util/comparator.cc util/arena.cc)
SET(storage_src storage/iterator.cc storage/memtable.cc storage/dbformat.cc)
SET(dbms_src dbms/rtidb_tablet_server_impl.cc)
SET(rtidb_src rtidb_main.cc)
set(proto_src rtidb_tablet_server.proto)
set(protoc ${PROJECT_SOURCE_DIR}/thirdparty/bin/protoc)
set(protoc_flag --cpp_out)

foreach(proto_file ${proto_src})
    get_filename_component(proto_name ${proto_file} NAME_WE)
    set(cur_proto_gen 
        ${proto_name}.pb.h
        ${proto_name}.pb.cc)
    set(tablet_src proto/${proto_name}.pb.cc)
    add_custom_command(
        OUTPUT ${cur_proto_gen}
        COMMAND ${protoc} ${proto_file} ${protoc_flag} .  -I . 
        DEPENDS ${protoc} ${proto_file}
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/src/proto
    )
endforeach(proto_file ${proto_src})
add_library(rtidb_tablet_server_pb STATIC ${tablet_src})

ADD_EXECUTABLE(rtidb ${rtidb_src} ${storage_src} ${util_src} ${dbms_src})
TARGET_LINK_LIBRARIES(rtidb pthread rtidb_tablet_server_pb protobuf)

