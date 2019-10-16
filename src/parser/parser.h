/*
 * parser/parser.h
 * Copyright (C) 4paradigm.com 2019 chenjing <chenjing@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FEDB_SQL_PARSER_H_
#define FEDB_SQL_PARSER_H_

#include "parser/node.h"
#include "parser/sql_parser.gen.h"

#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif

#ifndef YY_TYPEDEF_YY_BUFFER_STATE
#define YY_TYPEDEF_YY_BUFFER_STATE
typedef struct yy_buffer_state * YY_BUFFER_STATE;
#endif


int FeSqlParse(const char* sqlstr, ::fedb::sql::SQLNodeList* list);

extern YY_BUFFER_STATE yy_scan_string ( const char *yy_str , yyscan_t yyscanner );
extern int yylex_init (yyscan_t* scanner);
extern int yyparse (yyscan_t scanner, const ::fedb::sql::SQLNodeList *nodelist);
extern int yylex_destroy ( yyscan_t yyscanner );
#endif /* !FEDB_SQL_PARSER_H_ */
