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

add_library(sql_parser ${CMAKE_CURRENT_BINARY_DIR}/sql_scan.gen.cc ${CMAKE_CURRENT_BINARY_DIR}/sql_parser.gen.cc node.cc parser.cc)
add_executable(sql_parser_test sql_parser_test.cc)
target_link_libraries(sql_parser_test sql_parser gtest pthread)
add_test(sql_parser_test sql_parser_test --gtest_output=xml:${CMAKE_BINARY_DIR}/sql_parser_test.xml)

add_executable(sql_node_test sql_node_test.cc node.cc)
target_link_libraries(sql_node_test gtest pthread)
add_test(sql_node_test sql_node_test --gtest_output=xml:${CMAKE_BINARY_DIR}/sql_node_test.xml)

add_executable(sql_list_node_test sql_list_node_test.cc node.cc)
target_link_libraries(sql_list_node_test gtest pthread)
add_test(sql_list_node_test sql_list_node_test --gtest_output=xml:${CMAKE_BINARY_DIR}/sql_list_node_test.xml)

