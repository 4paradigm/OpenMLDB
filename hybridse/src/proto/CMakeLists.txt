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

include_directories(${INCLUDE_DIRECTORIES} ${PROJECT_SOURCE_DIR}/src
                    ${PROJECT_BINARY_DIR}/src)

set(PROTO_CPP_FILES "")
set(PROTO_FILES "")
function(compile_proto proto_name java_file_suffix_path)
  add_custom_command(
    OUTPUT
      ${CMAKE_CURRENT_BINARY_DIR}/${proto_name}.pb.cc
      ${CMAKE_CURRENT_BINARY_DIR}/${proto_name}.pb.h
      ${CMAKE_SOURCE_DIR}/java/hybridse-proto/src/main/java/com/_4paradigm/hybridse/${java_file_suffix_path}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
      -I ${PROJECT_SOURCE_DIR}/src/proto
      --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
      --java_out=${CMAKE_SOURCE_DIR}/java/hybridse-proto/src/main/java
      ${PROJECT_SOURCE_DIR}/src/proto/${proto_name}.proto
    DEPENDS ${PROJECT_SOURCE_DIR}/src/proto/${proto_name}.proto)
  list(APPEND PROTO_CPP_FILES ${CMAKE_CURRENT_BINARY_DIR}/${proto_name}.pb.cc)
  list(APPEND PROTO_FILES
    ${CMAKE_CURRENT_BINARY_DIR}/${proto_name}.pb.cc
    ${CMAKE_CURRENT_BINARY_DIR}/${proto_name}.pb.h
    ${CMAKE_SOURCE_DIR}/java/hybridse-proto/src/main/java/com/_4paradigm/hybridse/${java_file_suffix_path}
  )
  set(PROTO_CPP_FILES ${PROTO_CPP_FILES} PARENT_SCOPE)
  set(PROTO_FILES ${PROTO_FILES} PARENT_SCOPE)
endfunction(compile_proto)

compile_proto(fe_type type/TypeOuterClass.java)
compile_proto(fe_common common/Common.java)
compile_proto(dbms dbms/DBMS.java)
compile_proto(fe_tablet tablet/Tablet.java)
compile_proto(batch batch/Batch.java)
compile_proto(plan batch/Plan.java)

add_library(hybridse_proto OBJECT ${PROTO_CPP_FILES})
set_property(
  TARGET hybridse_proto
  APPEND
  PROPERTY ADDITIONAL_CLEAN_FILES ${PROTO_FILES}
)
