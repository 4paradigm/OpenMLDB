include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

set(TYPE_PB_CC
        ${CMAKE_CURRENT_BINARY_DIR}/type.pb.cc)

set(COMMON_PB_CC
        ${CMAKE_CURRENT_BINARY_DIR}/common.pb.cc
        )

set(DBMS_PB_CC
        ${CMAKE_CURRENT_BINARY_DIR}/dbms.pb.cc)

set(TABLET_PB_CC
    ${CMAKE_CURRENT_BINARY_DIR}/tablet.pb.cc)

add_custom_command(OUTPUT ${COMMON_PB_CC}
        COMMAND ${PROJECT_SOURCE_DIR}/thirdparty/bin/protoc -I ${PROJECT_SOURCE_DIR}/src/proto --cpp_out=${CMAKE_CURRENT_BINARY_DIR} ${PROJECT_SOURCE_DIR}/src/proto/common.proto
        )

add_custom_command(OUTPUT ${TYPE_PB_CC}
        COMMAND ${PROJECT_SOURCE_DIR}/thirdparty/bin/protoc -I ${PROJECT_SOURCE_DIR}/src/proto --cpp_out=${CMAKE_CURRENT_BINARY_DIR} ${PROJECT_SOURCE_DIR}/src/proto/type.proto
        )

add_custom_command(OUTPUT ${DBMS_PB_CC}
        COMMAND ${PROJECT_SOURCE_DIR}/thirdparty/bin/protoc -I ${PROJECT_SOURCE_DIR}/src/proto --cpp_out=${CMAKE_CURRENT_BINARY_DIR} ${PROJECT_SOURCE_DIR}/src/proto/dbms.proto
        )

add_custom_command(OUTPUT ${TABLET_PB_CC}
        COMMAND ${PROJECT_SOURCE_DIR}/thirdparty/bin/protoc -I ${PROJECT_SOURCE_DIR}/src/proto --cpp_out=${CMAKE_CURRENT_BINARY_DIR} ${PROJECT_SOURCE_DIR}/src/proto/tablet.proto
)

ADD_CUSTOM_TARGET(run_gen_proto DEPENDS ${TABLET_PB_CC} ${DBMS_PB_CC}
    ${COMMON_PB_CC}
    ${TYPE_PB_CC})

add_library(fesql_proto STATIC ${TYPE_PB_CC} ${COMMON_PB_CC} ${DBMS_PB_CC}
    ${TABLET_PB_CC})
