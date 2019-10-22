include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)

set(TYPE_PB_CC
    ${CMAKE_CURRENT_BINARY_DIR}/type.pb.cc)


add_custom_command(OUTPUT ${TYPE_PB_CC}
    COMMAND ${PROJECT_SOURCE_DIR}/thirdparty/bin/protoc -I ${PROJECT_SOURCE_DIR}/src/proto --cpp_out=${CMAKE_CURRENT_BINARY_DIR} ${PROJECT_SOURCE_DIR}/src/proto/type.proto
)

add_library(fesql_proto STATIC ${TYPE_PB_CC})
