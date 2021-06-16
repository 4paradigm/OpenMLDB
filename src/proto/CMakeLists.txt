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

include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

set(TYPE_PB_CC
        ${CMAKE_CURRENT_BINARY_DIR}/fe_type.pb.cc)

set(COMMON_PB_CC
        ${CMAKE_CURRENT_BINARY_DIR}/fe_common.pb.cc
        )

set(DBMS_PB_CC
        ${CMAKE_CURRENT_BINARY_DIR}/dbms.pb.cc)

set(TABLET_PB_CC
    ${CMAKE_CURRENT_BINARY_DIR}/fe_tablet.pb.cc)

set(BATCH_PB_CC
    ${CMAKE_CURRENT_BINARY_DIR}/batch.pb.cc)

set(PLAN_PB_CC
    ${CMAKE_CURRENT_BINARY_DIR}/plan.pb.cc)

add_custom_command(OUTPUT ${COMMON_PB_CC}
        COMMAND ${Protobuf_PROTOC_EXECUTABLE} -I ${PROJECT_SOURCE_DIR}/src/proto
        --cpp_out=${CMAKE_CURRENT_BINARY_DIR} ${PROJECT_SOURCE_DIR}/src/proto/fe_common.proto
        DEPENDS ${PROJECT_SOURCE_DIR}/src/proto/fe_common.proto
        )

add_custom_command(OUTPUT ${TYPE_PB_CC}
        COMMAND ${Protobuf_PROTOC_EXECUTABLE} -I ${PROJECT_SOURCE_DIR}/src/proto --cpp_out=${CMAKE_CURRENT_BINARY_DIR} ${PROJECT_SOURCE_DIR}/src/proto/fe_type.proto
        DEPENDS ${PROJECT_SOURCE_DIR}/src/proto/fe_type.proto
        )

add_custom_command(OUTPUT ${DBMS_PB_CC}
        COMMAND ${Protobuf_PROTOC_EXECUTABLE} -I ${PROJECT_SOURCE_DIR}/src/proto --cpp_out=${CMAKE_CURRENT_BINARY_DIR} ${PROJECT_SOURCE_DIR}/src/proto/dbms.proto
        DEPENDS ${PROJECT_SOURCE_DIR}/src/proto/dbms.proto
        )

add_custom_command(OUTPUT ${TABLET_PB_CC}
        COMMAND ${Protobuf_PROTOC_EXECUTABLE} -I ${PROJECT_SOURCE_DIR}/src/proto --cpp_out=${CMAKE_CURRENT_BINARY_DIR} ${PROJECT_SOURCE_DIR}/src/proto/fe_tablet.proto
        DEPENDS ${PROJECT_SOURCE_DIR}/src/proto/fe_tablet.proto
        )

add_custom_command(OUTPUT ${BATCH_PB_CC}
        COMMAND ${Protobuf_PROTOC_EXECUTABLE} -I ${PROJECT_SOURCE_DIR}/src/proto
        --cpp_out=${CMAKE_CURRENT_BINARY_DIR} ${PROJECT_SOURCE_DIR}/src/proto/batch.proto
        DEPENDS ${PROJECT_SOURCE_DIR}/src/proto/batch.proto
        )

add_custom_command(OUTPUT ${PLAN_PB_CC}
        COMMAND ${Protobuf_PROTOC_EXECUTABLE} -I ${PROJECT_SOURCE_DIR}/src/proto --cpp_out=${CMAKE_CURRENT_BINARY_DIR} ${PROJECT_SOURCE_DIR}/src/proto/plan.proto
        DEPENDS ${PROJECT_SOURCE_DIR}/src/proto/plan.proto
        )

set(HYBRIDSE_PROTO_SRC_FILES
    ${TABLET_PB_CC}
    ${DBMS_PB_CC}
    ${COMMON_PB_CC}
    ${TYPE_PB_CC}
    ${BATCH_PB_CC}
    ${PLAN_PB_CC})

ADD_CUSTOM_TARGET(run_gen_proto DEPENDS ${HYBRIDSE_PROTO_SRC_FILES})

add_library(hybridse_proto OBJECT ${HYBRIDSE_PROTO_SRC_FILES})
add_dependencies(hybridse_proto run_gen_proto)

add_custom_target(hybridse_proto_java
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        -I ${PROJECT_SOURCE_DIR}/src/proto
        --java_out=${PROJECT_SOURCE_DIR}/java/hybridse-proto/src/main/java
        ${PROJECT_SOURCE_DIR}/src/proto/*.proto)

