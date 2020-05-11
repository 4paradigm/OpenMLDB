include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

set(SQL_AST_LEXER_GEN
        ${CMAKE_CURRENT_BINARY_DIR}/sql_scan.gen.cc)

set(SQL_AST_PARSER_GEN
        ${CMAKE_CURRENT_BINARY_DIR}/sql_parser.gen.cc)
set(case_deps_libs fesql_case ${yaml_libs} fesql_codec boost_filesystem)
add_custom_command(OUTPUT ${SQL_AST_LEXER_GEN}
        COMMAND flex ${PROJECT_SOURCE_DIR}/src/parser/sql_scan.l
        )

add_custom_command(OUTPUT ${SQL_AST_PARSER_GEN}
        COMMAND ${PROJECT_SOURCE_DIR}/thirdparty/bin/bison ${PROJECT_SOURCE_DIR}/src/parser/sql_parser.y
        )

ADD_CUSTOM_TARGET(run_gen_parser DEPENDS ${SQL_AST_LEXER_GEN}
    ${SQL_AST_PARSER_GEN})

add_library(fesql_parser STATIC ${CMAKE_CURRENT_BINARY_DIR}/sql_scan.gen.cc ${CMAKE_CURRENT_BINARY_DIR}/sql_parser.gen.cc parser.cc)

if (TESTING_ENABLE)
add_executable(sql_parser_test sql_parser_test.cc)
target_link_libraries(sql_parser_test gtest fesql_parser fesql_node ${case_deps_libs} fesql_proto ${BRPC_LIBS} ${OS_LIB})
add_test(sql_parser_test sql_parser_test --gtest_output=xml:${CMAKE_BINARY_DIR}/sql_parser_test.xml)
endif()
