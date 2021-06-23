/*
 * ast_node_converter.h
 * Copyright (C) 4paradigm 2021 chenjing <chenjing@4paradigm.com>
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
#ifndef SRC_PLANV2_AST_NODE_CONVERTER_H_
#define SRC_PLANV2_AST_NODE_CONVERTER_H_
#include <string>

#include "node/node_manager.h"
#include "udf/udf.h"
#include "zetasql/parser/parser.h"

namespace hybridse {
namespace plan {
base::Status ConvertASTType(const zetasql::ASTType* ast_type, node::NodeManager* node_manager, node::DataType* output);
base::Status ConvertExprNode(const zetasql::ASTExpression* ast_expression, node::NodeManager* node_manager,
                             node::ExprNode** output);

base::Status ConvertStatement(const zetasql::ASTStatement* stmt, node::NodeManager* node_manager,
                              node::SqlNode** output);

base::Status ConvertOrderBy(const zetasql::ASTOrderBy* order_by, node::NodeManager* node_manager,
                            node::OrderByNode** output);

base::Status ConvertDotStart(const zetasql::ASTDotStar* dot_start_expression, node::NodeManager* node_manager,
                             node::ExprNode** output);
base::Status ConvertExprNodeList(const absl::Span<const zetasql::ASTExpression* const>& expression_list,
                                 node::NodeManager* node_manager, node::ExprListNode** output);
base::Status ConvertFrameBound(const zetasql::ASTWindowFrameExpr* window_frame_expr, node::NodeManager* node_manager,
                               node::FrameBound** output);
base::Status ConvertFrameNode(const zetasql::ASTWindowFrame* window_frame, node::NodeManager* node_manager,
                              node::FrameNode** output);
base::Status ConvertWindowDefinition(const zetasql::ASTWindowDefinition* window_definition,
                                     node::NodeManager* node_manager, node::WindowDefNode** output);
base::Status ConvertWindowSpecification(const zetasql::ASTWindowSpecification* window_spec,
                                        node::NodeManager* node_manager, node::WindowDefNode** output);
base::Status ConvertWindowClause(const zetasql::ASTWindowClause* window_clause, node::NodeManager* node_manager,
                                 node::SqlNodeList** output);
base::Status ConvertTableExpressionNode(const zetasql::ASTTableExpression* root, node::NodeManager* node_manager,
                                        node::TableRefNode** output);
base::Status ConvertSelectList(const zetasql::ASTSelectList* select_list, node::NodeManager* node_manager,
                               node::SqlNodeList** output);
base::Status ConvertLimitOffsetNode(const zetasql::ASTLimitOffset* limit_offset, node::NodeManager* node_manager,
                                    node::SqlNode** output);

base::Status ConvertQueryNode(const zetasql::ASTQuery* root, node::NodeManager* node_manager, node::QueryNode** output);

base::Status ConvertQueryExpr(const zetasql::ASTQueryExpression* query_expr, node::NodeManager* node_manager,
                              node::QueryNode** output);

/// transform zetasql::ASTCreateStatement into CreateStmt
base::Status ConvertCreateTableNode(const zetasql::ASTCreateTableStatement* ast_create_stmt,
                                    node::NodeManager* node_manager, node::CreateStmt** output);

base::Status ConvertCreateProcedureNode(const zetasql::ASTCreateProcedureStatement* ast_create_sp_stmt,
                                        node::NodeManager* node_manager, node::CreateSpStmt** output);

base::Status ConvertParamter(const zetasql::ASTFunctionParameter* params, node::NodeManager* node_manager,
                             node::SqlNode** output);

base::Status ConvertASTScript(const zetasql::ASTScript* body, node::NodeManager* node_manager,
                              node::SqlNodeList** output);
base::Status ConvertProcedureBody(const zetasql::ASTScript* body, node::NodeManager* node_manager,
                                  node::SqlNodeList** output);
/// transform zetasql::ASTTableElement into corresponding SqlNode
base::Status ConvertTableElement(const zetasql::ASTTableElement* ast_table_element, node::NodeManager* node_manager,
                                 node::SqlNode** node);

/// transform zetasql::ASTIndexDefinition into ColumnIndexNode
base::Status ConvertColumnIndexNode(const zetasql::ASTIndexDefinition* ast_def_node, node::NodeManager* node_manager,
                                    node::ColumnIndexNode** output);

base::Status ConvertIndexOption(const zetasql::ASTOptionsEntry* entry, node::NodeManager* node_manager,
                                node::SqlNode** output);

base::Status ConvertTableOption(const zetasql::ASTOptionsEntry* entry, node::NodeManager* node_manager,
                                node::SqlNode** output);

// utility function
base::Status AstStringLiteralToString(const zetasql::ASTExpression* ast_expr, std::string* str);
base::Status AstPathExpressionToString(const zetasql::ASTPathExpression* ast_expr, std::string* str);
base::Status AstPathExpressionToStringList(const zetasql::ASTPathExpression* ast_expr,
                                           std::vector<std::string>& strs);  // NOLINT

base::Status ASTIntLiteralToNum(const zetasql::ASTExpression* ast_expr, int64_t* val);
base::Status ASTIntervalLIteralToNum(const zetasql::ASTExpression* ast_expr, int64_t* val, node::DataType* unit);

base::Status ConvertInsertStatement(const zetasql::ASTInsertStatement* root, node::NodeManager* node_manager,
                                    node::InsertStmt** output);
base::Status ConvertDropStatement(const zetasql::ASTDropStatement* root, node::NodeManager* node_manager,
                                  node::CmdNode** output);
}  // namespace plan
}  // namespace hybridse
#endif  // SRC_PLANV2_AST_NODE_CONVERTER_H_
