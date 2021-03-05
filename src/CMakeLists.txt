include_directories(${INCLUDE_DIRECTORIES} ${PROJECT_SOURCE_DIR}/src)

function(compile_proto proto_name project_dir)
add_custom_command(OUTPUT ${project_dir}/src/proto/${proto_name}.pb.cc
        COMMAND ${project_dir}/thirdparty/bin/protoc -I ${PROJECT_SOURCE_DIR}/src/proto
        --cpp_out=${project_dir}/src/proto/ ${project_dir}/src/proto/${proto_name}.proto
        DEPENDS ${project_dir}/src/proto/${proto_name}.proto
        )
endfunction(compile_proto)

function(compile_lib LIB_NAME DIR DEPEND_FILE_LIST)
    set(FILE_STR_LIST "")
    file(GLOB_RECURSE SRC_FILES ${DIR}/*.cc)
    foreach(SRC_FILE ${SRC_FILES})
        if (NOT SRC_FILE MATCHES ".*_test.cc")
            set(FILE_STR_LIST "${FILE_STR_LIST} ${SRC_FILE}")
        endif()
    endforeach()
    set(FILE_STR_LIST "${FILE_STR_LIST} ${DEPEND_FILE_LIST}")
    string(REPLACE " " ";" FILE_LIST ${FILE_STR_LIST})
    add_library(${LIB_NAME} STATIC ${FILE_LIST} $<TARGET_OBJECTS:rtidb_proto>)
endfunction(compile_lib)

function(compile_test DIR)
    set(TEST_LIBS nameserver tablet rtidb_sdk blobserver blob_proxy rtidb_catalog client zk_client replica base beans storage rtidb_codec rtidb_proto log common rocksdb zookeeper_mt tcmalloc_minimal
    ${VM_LIBS}
    ${LLVM_LIBS}
    ${BRPC_LIBS})
    file(GLOB_RECURSE SRC_FILES ${DIR}/*.cc)
    set(TEST_CPP_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
    foreach(SRC_FILE ${SRC_FILES})
        if (SRC_FILE MATCHES ".*_test.cc")
            file(RELATIVE_PATH RELATIVE_TEST_PATH ${TEST_CPP_SOURCE_DIR} ${SRC_FILE})
            get_filename_component(TEST_TARGET_DIR ${RELATIVE_TEST_PATH} DIRECTORY)
            get_filename_component(TEST_TARGET_NAME ${RELATIVE_TEST_PATH} NAME_WE)
            add_executable(${TEST_TARGET_NAME} ${SRC_FILE} $<TARGET_OBJECTS:rtidb_proto>)
            if (${TEST_TARGET_NAME} STREQUAL "tablet_impl_mem_test")
                target_link_libraries(${TEST_TARGET_NAME} tcmalloc ${TEST_LIBS} gtest)
            elseif (${TEST_TARGET_NAME} STREQUAL "tablet_engine_test")
                target_link_libraries(${TEST_TARGET_NAME} base_test ${TEST_LIBS} yaml-cpp boost_filesystem gtest)
            else()
                target_link_libraries(${TEST_TARGET_NAME} ${TEST_LIBS} gtest)
            endif()
        endif()
    endforeach()
endfunction(compile_test)

compile_proto(type ${PROJECT_SOURCE_DIR})
compile_proto(client ${PROJECT_SOURCE_DIR})
compile_proto(common ${PROJECT_SOURCE_DIR})
compile_proto(tablet ${PROJECT_SOURCE_DIR})
compile_proto(blob_server ${PROJECT_SOURCE_DIR})
compile_proto(blob_proxy ${PROJECT_SOURCE_DIR})
compile_proto(name_server ${PROJECT_SOURCE_DIR})
compile_proto(sql_procedure ${PROJECT_SOURCE_DIR})

compile_lib(rtidb_codec codec "")
compile_lib(rtidb_catalog catalog "")
compile_lib(client client "")
compile_lib(base base "")
compile_lib(zk_client zk "")
compile_lib(tablet tablet "")
compile_lib(nameserver nameserver "")
compile_lib(storage storage "")
compile_lib(replica replica "")
compile_lib(log log "flags.cc")
compile_lib(blobserver blobserver "storage/object_store.cc flags.cc")
compile_lib(blob_proxy blob_proxy "")
compile_lib(rtidb_sdk sdk "")

add_library(rtidb_proto STATIC proto/type.pb.cc proto/client.pb.cc proto/common.pb.cc proto/tablet.pb.cc proto/blob_server.pb.cc proto/blob_proxy.pb.cc proto/name_server.pb.cc proto/sql_procedure.pb.cc)
add_library(beans storage/beans/bitcask.c storage/beans/clock_gettime_stub.c storage/beans/codec.c storage/beans/common.c storage/beans/crc32.c storage/beans/diskmgr.c storage/beans/hint.c storage/beans/hstore.c storage/beans/htree.c storage/beans/mfile.c storage/beans/quicklz.c storage/beans/record.c)

add_library(rtidb_flags flags.cc)

set(BIN_LIBS nameserver tablet rtidb_sdk blobserver blob_proxy rtidb_catalog client zk_client replica base beans storage rtidb_codec rtidb_proto log common rocksdb zookeeper_mt tcmalloc_minimal
${VM_LIBS}
${LLVM_LIBS}
${BRPC_LIBS})

if(TESTING_ENABLE)
    compile_test(base)
    compile_test(codec)
    compile_test(zk)
    compile_test(tablet)
    compile_test(nameserver)
    compile_test(storage)
    compile_test(replica)
    compile_test(catalog)
    compile_test(log)
    compile_test(blobserver)
    compile_test(blob_proxy)
endif()

add_executable(parse_log tools/parse_log.cc  $<TARGET_OBJECTS:rtidb_proto>)
target_link_libraries(parse_log log rtidb_proto base protobuf glog gflags unwind lzma ssl crypto z snappy dl pthread)

add_executable(rtidb cmd/rtidb.cc base/status.cc proto/client.pb.cc base/linenoise.cc)
target_link_libraries(rtidb ${BIN_LIBS})

add_subdirectory(client)
add_subdirectory(sdk)
