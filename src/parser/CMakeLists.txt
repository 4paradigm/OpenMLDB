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
        DEPENDS ${PROJECT_SOURCE_DIR}/src/parser/sql_scan.l
        )

add_custom_command(OUTPUT ${SQL_AST_PARSER_GEN}
        COMMAND ${PROJECT_SOURCE_DIR}/thirdparty/bin/bison ${PROJECT_SOURCE_DIR}/src/parser/sql_parser.y
        DEPENDS ${PROJECT_SOURCE_DIR}/src/parser/sql_parser.y
        )

ADD_CUSTOM_TARGET(run_gen_parser DEPENDS ${SQL_AST_LEXER_GEN}
    ${SQL_AST_PARSER_GEN})

add_library(fesql_parser OBJECT ${SQL_AST_LEXER_GEN} ${SQL_AST_PARSER_GEN})
add_dependencies(fesql_parser run_gen_parser)


