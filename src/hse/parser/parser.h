/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_PARSER_PARSER_H_
#define SRC_PARSER_PARSER_H_

#include <iostream>
#include <list>
#include <map>
#include <string>
#include <vector>
#include "base/fe_status.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "parser/sql_parser.gen.h"
#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void *yyscan_t;
#endif

#ifndef YY_TYPEDEF_YY_BUFFER_STATE
#define YY_TYPEDEF_YY_BUFFER_STATE
typedef struct yy_buffer_state *YY_BUFFER_STATE;
#endif

namespace hybridse {
namespace parser {

class HybridSeParser {
 public:
    HybridSeParser() {}

    int parse(const std::string &sqlstr,
              node::NodePointVector &trees,  // NOLINT (runtime/references)
              node::NodeManager *manager,
              base::Status &status);  // NOLINT (runtime/references)
 private:
    int CreateFnBlock(std::vector<node::FnNode *> vector, int start, int end,
                      int32_t indent, node::FnNodeList *block,
                      node::NodeManager *node_manager,
                      base::Status &status);  // NOLINT (runtime/references)
    int ReflectFnDefNode(node::FnNodeFnDef *fn_def,
                         node::NodeManager *node_manager,
                         base::Status &status);  // NOLINT (runtime/references)
    // NOLINT
    bool SSAOptimized(const node::FnNodeList *block,
                      std::map<std::string, node::FnNode *>
                          &assign_var_map,    // NOLINT (runtime/references)
                      base::Status &status);  // NOLINT (runtime/references)
};
}  // namespace parser
}  // namespace hybridse

extern YY_BUFFER_STATE yy_scan_string(const char *yy_str, yyscan_t yyscanner);
extern int yylex_init(yyscan_t *scanner);
extern int yyparse(
    yyscan_t scanner,
    hybridse::node::NodePointVector &trees,  // NOLINT (runtime/references)
    ::hybridse::node::NodeManager *node_manager,
    ::hybridse::base::Status &status);  // NOLINT (runtime/references)
extern int yylex_destroy(yyscan_t yyscanner);
extern void yyset_lineno(int line_number, yyscan_t scanner);
extern void yyset_column(int line_number, yyscan_t scanner);
extern int yyget_lineno(yyscan_t scanner);
extern int yyget_column(yyscan_t scanner);
extern char *yyget_text(yyscan_t scanner);

#endif  // SRC_PARSER_PARSER_H_
