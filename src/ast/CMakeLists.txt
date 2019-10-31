include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)

set(FN_AST_LEXER_GEN
    ${CMAKE_CURRENT_BINARY_DIR}/fn_lexer.gen.cc)

set(FN_AST_PARSER_GEN
    ${CMAKE_CURRENT_BINARY_DIR}/fn_parser.gen.h
    ${CMAKE_CURRENT_BINARY_DIR}/fn_parser.gen.cc)


add_custom_command(OUTPUT ${FN_AST_LEXER_GEN}
    COMMAND ${PROJECT_SOURCE_DIR}/thirdparty/bin/flex -P fn ${PROJECT_SOURCE_DIR}/src/ast/fn_lexer.l 
)

add_custom_command(OUTPUT ${FN_AST_PARSER_GEN}
    COMMAND ${PROJECT_SOURCE_DIR}/thirdparty/bin/bison -p fn ${PROJECT_SOURCE_DIR}/src/ast/fn_parser.y
)

add_library(fesql_fn_ast STATIC ${CMAKE_CURRENT_BINARY_DIR}/fn_lexer.gen.cc ${CMAKE_CURRENT_BINARY_DIR}/fn_parser.gen.cc)

add_executable(fn_parser_test fn_parser_test.cc)
target_link_libraries(fn_parser_test fesql_fn_ast fesql_node glog gtest pthread)
add_test(fn_parser_test fn_parser_test --gtest_output=xml:${CMAKE_BINARY_DIR}/fn_parser_test.xml)
list(APPEND fn_parser_test fn_parser_test)
