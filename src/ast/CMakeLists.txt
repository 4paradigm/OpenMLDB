include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)
set(AST_LEXER_GEN
    ${CMAKE_CURRENT_BINARY_DIR}/lexer.gen.cc)

set(AST_PARSER_GEN
    ${CMAKE_CURRENT_BINARY_DIR}/parser.gen.h
    ${CMAKE_CURRENT_BINARY_DIR}/parser.gen.cc)

add_custom_command(OUTPUT ${AST_LEXER_GEN}
    COMMAND ${PROJECT_SOURCE_DIR}/thirdparty/bin/flex ${PROJECT_SOURCE_DIR}/src/ast/lexer.l 
)

add_custom_command(OUTPUT ${AST_PARSER_GEN}
    COMMAND ${PROJECT_SOURCE_DIR}/thirdparty/bin/bison ${PROJECT_SOURCE_DIR}/src/ast/parser.y
)

add_library(ast STATIC ${CMAKE_CURRENT_BINARY_DIR}/lexer.gen.cc ${CMAKE_CURRENT_BINARY_DIR}/parser.gen.cc)
add_executable(ast_test ast_test.cc)
target_link_libraries(ast_test ast gtest pthread)
add_test(ast_test ast_test)
