include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

set(SQL_AST_LEXER_GEN
        ${CMAKE_CURRENT_BINARY_DIR}/sql_scan.gen.cc)

set(SQL_AST_PARSER_GEN
        ${CMAKE_CURRENT_BINARY_DIR}/sql_parser.gen.h
        ${CMAKE_CURRENT_BINARY_DIR}/sql_parser.gen.cc)

add_custom_command(OUTPUT ${SQL_AST_LEXER_GEN}
        COMMAND flex ${PROJECT_SOURCE_DIR}/src/parser/sql_scan.l
        )

add_custom_command(OUTPUT ${SQL_AST_PARSER_GEN}
        COMMAND bison ${PROJECT_SOURCE_DIR}/src/parser/sql_parser.y
        )

add_library(fesql_parser STATIC  ${CMAKE_CURRENT_BINARY_DIR}/sql_scan.gen.cc ${CMAKE_CURRENT_BINARY_DIR}/sql_parser.gen.cc parser.cc)
add_executable(sql_parser_test sql_parser_test.cc)
target_link_libraries(sql_parser_test fesql_parser fesql_node ${COMMON_LIBS} gtest pthread)
add_test(sql_parser_test sql_parser_test --gtest_output=xml:${CMAKE_BINARY_DIR}/sql_parser_test.xml)
list(APPEND test_list sql_parser_test)


