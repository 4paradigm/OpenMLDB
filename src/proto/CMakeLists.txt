include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

set(TYPE_PB_CC
        ${CMAKE_CURRENT_BINARY_DIR}/type.pb.cc
        ${CMAKE_CURRENT_BINARY_DIR}/type.pb.h)

set(COMMON_PB_CC
        ${CMAKE_CURRENT_BINARY_DIR}/common.pb.cc
        ${CMAKE_CURRENT_BINARY_DIR}/common.pb.h)

set(DBMS_PB_CC
        ${CMAKE_CURRENT_BINARY_DIR}/dbms.pb.cc
        ${CMAKE_CURRENT_BINARY_DIR}/dbms.pb.h)

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
    COMMAND ${PROJECT_SOURCE_DIR}/thirdparty/bin/protoc -I
    ${PROJECT_SOURCE_DIR}/src/proto --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
    ${PROJECT_SOURCE_DIR}/src/proto/tablet.proto
)

add_library(fesql_proto STATIC ${TYPE_PB_CC} ${COMMON_PB_CC} ${DBMS_PB_CC}
    ${TABLET_PB_CC})
