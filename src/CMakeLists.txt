# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include_directories(${INCLUDE_DIRECTORIES} ${PROJECT_SOURCE_DIR}/src)

add_subdirectory(statistics)

function(compile_proto proto_name project_dir)
add_custom_command(OUTPUT ${project_dir}/src/proto/${proto_name}.pb.cc
        COMMAND ${Protobuf_PROTOC_EXECUTABLE} -I ${PROJECT_SOURCE_DIR}/src/proto
        --cpp_out=${project_dir}/src/proto/
        --java_out=${project_dir}/java/openmldb-native/src/main/java
        --java_out=${project_dir}/java/openmldb-import/src/main/java
        --java_out=${project_dir}/java/openmldb-taskmanager/src/main/java
        --java_out=${project_dir}/java/openmldb-common/src/main/java
        ${project_dir}/src/proto/${proto_name}.proto
        DEPENDS ${project_dir}/src/proto/${proto_name}.proto
        )
endfunction(compile_proto)

function(compile_lib LIB_NAME DIR DEPEND_FILE_LIST)
    set(FILE_STR_LIST "")
    file(GLOB_RECURSE SRC_FILES ${DIR}/*.cc)
    foreach(SRC_FILE ${SRC_FILES})
        if (NOT SRC_FILE MATCHES ".*_test.cc" AND NOT SRC_FILE MATCHES ".*_bm.cc")
            set(FILE_STR_LIST "${FILE_STR_LIST} ${SRC_FILE}")
        endif()
    endforeach()
    set(FILE_STR_LIST "${FILE_STR_LIST} ${DEPEND_FILE_LIST}")
    string(REPLACE " " ";" FILE_LIST ${FILE_STR_LIST})
    add_library(${LIB_NAME} STATIC ${FILE_LIST} $<TARGET_OBJECTS:openmldb_proto>)
endfunction(compile_lib)

function(compile_test DIR)
    set(TEST_LIBS apiserver nameserver tablet query_response_time openmldb_sdk openmldb_catalog schema client zk_client replica base storage openmldb_codec openmldb_proto log common zookeeper_mt tcmalloc_minimal gflags ${RocksDB_LIB}
    ${VM_LIBS}
    ${LLVM_LIBS}
    ${ZETASQL_LIBS}
    ${BRPC_LIBS})
    file(GLOB_RECURSE SRC_FILES ${DIR}/*.cc)
    foreach(SRC_FILE ${SRC_FILES})
        if (SRC_FILE MATCHES ".*_test.cc")
            file(RELATIVE_PATH RELATIVE_TEST_PATH ${CMAKE_CURRENT_SOURCE_DIR} ${SRC_FILE})
            get_filename_component(TEST_TARGET_DIR ${RELATIVE_TEST_PATH} DIRECTORY)
            get_filename_component(TEST_TARGET_NAME ${RELATIVE_TEST_PATH} NAME_WE)
            add_executable(${TEST_TARGET_NAME} ${SRC_FILE} $<TARGET_OBJECTS:openmldb_proto>)
            list(APPEND test_list ${TEST_TARGET_NAME})
            if (${TEST_TARGET_NAME} STREQUAL "tablet_impl_mem_test")
                target_link_libraries(${TEST_TARGET_NAME} tcmalloc ${TEST_LIBS} ${GTEST_LIBRARIES})
            else()
                target_link_libraries(${TEST_TARGET_NAME} ${TEST_LIBS} ${GTEST_LIBRARIES})
            endif()
            set_target_properties(${TEST_TARGET_NAME}
                PROPERTIES
                RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR})
            add_test(${TEST_TARGET_NAME}
                ${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR}/${TEST_TARGET_NAME}
                --gtest_output=xml:${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR}/${TEST_TARGET_NAME}.xml)
        endif()
    endforeach()
    set(test_list ${test_list} PARENT_SCOPE)
endfunction(compile_test)

compile_proto(type ${PROJECT_SOURCE_DIR})
compile_proto(name_server ${PROJECT_SOURCE_DIR})
compile_proto(common ${PROJECT_SOURCE_DIR})
compile_proto(tablet ${PROJECT_SOURCE_DIR})
compile_proto(name_server ${PROJECT_SOURCE_DIR})
compile_proto(sql_procedure ${PROJECT_SOURCE_DIR})
compile_proto(api_server ${PROJECT_SOURCE_DIR})
compile_proto(taskmanager ${PROJECT_SOURCE_DIR})

compile_lib(openmldb_codec codec "")
compile_lib(openmldb_catalog catalog "")
compile_lib(schema schema "")
compile_lib(client client "")
compile_lib(base base "")
compile_lib(zk_client zk "")
compile_lib(tablet tablet "")
compile_lib(nameserver nameserver "")
compile_lib(storage storage "")
compile_lib(replica replica "")
compile_lib(log log "flags.cc")
compile_lib(openmldb_sdk sdk "")
compile_lib(apiserver apiserver "")

add_library(openmldb_proto STATIC proto/type.pb.cc proto/common.pb.cc proto/tablet.pb.cc proto/name_server.pb.cc proto/sql_procedure.pb.cc proto/api_server.pb.cc proto/taskmanager.pb.cc proto/name_server.pb.cc)

add_library(openmldb_flags flags.cc)

set(BIN_LIBS apiserver nameserver tablet query_response_time openmldb_sdk openmldb_catalog client zk_client replica base storage openmldb_codec schema openmldb_proto log common zookeeper_mt tcmalloc_minimal ${RocksDB_LIB}
${VM_LIBS}
${LLVM_LIBS}
${ZETASQL_LIBS}
${BRPC_LIBS})

if(TESTING_ENABLE)
    compile_test(cmd)
    compile_test(base)
    compile_test(codec)
    compile_test(zk)
    compile_test(tablet)
    compile_test(nameserver)
    compile_test(storage)
    compile_test(replica)
    compile_test(catalog)
    compile_test(schema)
    compile_test(log)
    compile_test(apiserver)
    add_library(test_udf SHARED examples/test_udf.cc)
endif()

add_executable(parse_log tools/parse_log.cc  $<TARGET_OBJECTS:openmldb_proto>)

set(LINK_LIBS log openmldb_proto base ${PROTOBUF_LIBRARY} ${GLOG_LIBRARY} ${GFLAGS_LIBRARY} ${OPENSSL_LIBRARIES} ${Z_LIBRARY} ${SNAPPY_LIBRARY} dl pthread)
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    list(APPEND LINK_LIBS unwind)
endif()
target_link_libraries(parse_log ${LINK_LIBS})

add_executable(openmldb cmd/openmldb.cc base/linenoise.cc)
target_link_libraries(openmldb ${BIN_LIBS})

install(
    TARGETS openmldb
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

add_subdirectory(sdk)

set(test_list ${test_list} PARENT_SCOPE)
