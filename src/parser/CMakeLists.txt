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

include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

set(SQL_AST_LEXER_GEN
        ${CMAKE_CURRENT_BINARY_DIR}/sql_scan.gen.cc)

set(SQL_AST_PARSER_GEN
        ${CMAKE_CURRENT_BINARY_DIR}/sql_parser.gen.cc)
set(case_deps_libs hybridse_case ${yaml_libs} hybridse_codec boost_filesystem)
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

add_library(hybridse_parser OBJECT ${SQL_AST_LEXER_GEN} ${SQL_AST_PARSER_GEN})
add_dependencies(hybridse_parser run_gen_parser)

