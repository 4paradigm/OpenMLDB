/*
 * fn_parser.h
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
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

#ifndef AST_FN_PARSER_H_
#define AST_FN_PARSER_H_

#include "ast/fn_ast.h"
#include "node/node_memory.h"
#include "ast/fn_parser.gen.h"

#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif

#ifndef YY_TYPEDEF_YY_BUFFER_STATE
#define YY_TYPEDEF_YY_BUFFER_STATE
typedef struct yy_buffer_state *YY_BUFFER_STATE;
#endif


extern YY_BUFFER_STATE fn_scan_string ( const char *yy_str , yyscan_t yyscanner );
extern int fnlex_init (yyscan_t* scanner);
extern int fnparse (yyscan_t scanner, ::fesql::node::FnNode* root, ::fesql::node::NodeManager * node_manager);
extern int fnlex_destroy ( yyscan_t yyscanner );

#endif /* !AST_FN_PARSER_H_ */
