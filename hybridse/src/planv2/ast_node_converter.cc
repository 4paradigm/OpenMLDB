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
#include "planv2/ast_node_converter.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/match.h"
#include "base/fe_status.h"
#include "zetasql/parser/ast_node_kind.h"

namespace hybridse {
namespace plan {

template <typename NodeType, typename OutputType, typename ConvertFn>
ABSL_MUST_USE_RESULT
base::Status ConvertGuard(const zetasql::ASTNode* node, node::NodeManager* nm, OutputType** output, ConvertFn&& func) {
    auto specific_node = node->GetAsOrNull<NodeType>();
    CHECK_TRUE(specific_node != nullptr, common::kUnsupportSql, "not an ",
               zetasql::ASTNode::NodeKindToString(NodeType::kConcreteNodeKind));
    return func(specific_node, nm, output);
}

static base::Status convertShowStmt(const zetasql::ASTShowStatement* show_statement, node::NodeManager* node_manager,
                                    node::SqlNode** output);

static base::Status ConvertArrayExpr(const zetasql::ASTArrayConstructor* array_expr, node::NodeManager* nm,
                                     node::ExprNode** output);

static base::Status ConvertArrayType(const zetasql::ASTArrayConstructor* array_expr, node::NodeManager* nm,
                                     node::ExprNode** output);

static base::Status ConvertASTType(const zetasql::ASTType* ast_type, node::NodeManager* nm,  node::TypeNode** out);

static base::Status convertAlterAction(const zetasql::ASTAlterAction* action, node::NodeManager* nm,
                                       node::AlterActionBase** out);
static base::Status ConvertAlterTableStmt(const zetasql::ASTAlterTableStatement* stmt, node::NodeManager* nm,
                                          node::SqlNode** out);

/// Used to convert zetasql ASTExpression Node into our ExprNode
base::Status ConvertExprNode(const zetasql::ASTExpression* ast_expression, node::NodeManager* node_manager,
                             node::ExprNode** output) {
    if (nullptr == ast_expression) {
        *output = nullptr;
        return base::Status::OK();
    }
    base::Status status;
    switch (ast_expression->node_kind()) {
        case zetasql::AST_STAR: {
            *output = node_manager->MakeAllNode("");
            return base::Status::OK();
        }
        case zetasql::AST_DOT_STAR: {
            auto dot_start_expression = ast_expression->GetAsOrDie<zetasql::ASTDotStar>();
            CHECK_STATUS(ConvertDotStart(dot_start_expression, node_manager, output));
            return base::Status::OK();
        }
        case zetasql::AST_IDENTIFIER: {
            *output = node_manager->MakeExprIdNode(ast_expression->GetAsOrDie<zetasql::ASTIdentifier>()->GetAsString());
            return base::Status::OK();
        }
        case zetasql::AST_EXPRESSION_SUBQUERY: {
            auto expression_subquery = ast_expression->GetAsOrDie<zetasql::ASTExpressionSubquery>();
            node::QueryNode* subquery = nullptr;

            CHECK_STATUS(ConvertQueryNode(expression_subquery->query(), node_manager, &subquery))
            *output = node_manager->MakeQueryExprNode(subquery);
            return base::Status::OK();
        }
        case zetasql::AST_PATH_EXPRESSION: {
            auto* path_expression = ast_expression->GetAsOrDie<zetasql::ASTPathExpression>();
            int num_names = path_expression->num_names();
            if (1 == num_names) {
                *output = node_manager->MakeColumnRefNode(path_expression->first_name()->GetAsString(), "");
            } else if (2 == num_names) {
                *output = node_manager->MakeColumnRefNode(path_expression->name(1)->GetAsString(),
                                                          path_expression->name(0)->GetAsString());
            } else if (3 == num_names) {
                *output = node_manager->MakeColumnRefNode(path_expression->name(2)->GetAsString(),
                                                          path_expression->name(1)->GetAsString(),
                                                          path_expression->name(0)->GetAsString());
            } else {
                status.code = common::kSqlAstError;
                status.msg = "Invalid column path expression " + path_expression->ToIdentifierPathString();
                return status;
            }
            return base::Status::OK();
        }
        case zetasql::AST_CASE_VALUE_EXPRESSION: {
            auto* case_expression = ast_expression->GetAsOrDie<zetasql::ASTCaseValueExpression>();
            auto& arguments = case_expression->arguments();
            CHECK_TRUE(arguments.size() >= 3, common::kSqlAstError,
                       "Un-support case value expression with illegal argument size")
            node::ExprNode* value = nullptr;
            node::ExprListNode* when_list_expr = node_manager->MakeExprList();
            node::ExprNode* else_expr = nullptr;
            CHECK_STATUS(ConvertExprNode(arguments[0], node_manager, &value))
            size_t i = 1;
            while (i < arguments.size()) {
                if (i < arguments.size() - 1) {
                    node::ExprNode* when_expr = nullptr;
                    node::ExprNode* then_expr = nullptr;
                    CHECK_STATUS(ConvertExprNode(arguments[i], node_manager, &when_expr))
                    CHECK_STATUS(ConvertExprNode(arguments[i + 1], node_manager, &then_expr))
                    when_list_expr->PushBack(node_manager->MakeWhenNode(when_expr, then_expr));
                    i += 2;
                } else {
                    CHECK_STATUS(ConvertExprNode(arguments[i], node_manager, &else_expr))
                    i += 1;
                }
            }
            *output = node_manager->MakeSimpleCaseWhenNode(value, when_list_expr, else_expr);
            return base::Status::OK();
        }
        case zetasql::AST_CASE_NO_VALUE_EXPRESSION: {
            auto* case_expression = ast_expression->GetAsOrDie<zetasql::ASTCaseNoValueExpression>();
            auto& arguments = case_expression->arguments();
            CHECK_TRUE(arguments.size() >= 2, common::kSqlAstError,
                       "Un-support case value expression with ilegal argument size")
            node::ExprListNode* when_list_expr = node_manager->MakeExprList();
            node::ExprNode* else_expr = nullptr;
            size_t i = 0;
            while (i < arguments.size()) {
                if (i < arguments.size() - 1) {
                    node::ExprNode* when_expr = nullptr;
                    node::ExprNode* then_expr = nullptr;
                    CHECK_STATUS(ConvertExprNode(arguments[i], node_manager, &when_expr))
                    CHECK_STATUS(ConvertExprNode(arguments[i + 1], node_manager, &then_expr))
                    when_list_expr->PushBack(node_manager->MakeWhenNode(when_expr, then_expr));
                    i += 2;
                } else {
                    CHECK_STATUS(ConvertExprNode(arguments[i], node_manager, &else_expr))
                    i += 1;
                }
            }
            *output = node_manager->MakeSearchedCaseWhenNode(when_list_expr, else_expr);
            return base::Status::OK();
        }
        case zetasql::AST_BINARY_EXPRESSION: {
            auto* binary_expression = ast_expression->GetAsOrDie<zetasql::ASTBinaryExpression>();
            node::ExprNode* lhs = nullptr;
            node::ExprNode* rhs = nullptr;
            node::FnOperator op;

            CHECK_STATUS(ConvertExprNode(binary_expression->lhs(), node_manager, &lhs))
            CHECK_STATUS(ConvertExprNode(binary_expression->rhs(), node_manager, &rhs))
            switch (binary_expression->op()) {
                case zetasql::ASTBinaryExpression::Op::EQ: {
                    op = node::FnOperator::kFnOpEq;
                    break;
                }
                case zetasql::ASTBinaryExpression::Op::NE:
                case zetasql::ASTBinaryExpression::Op::NE2: {
                    op = node::FnOperator::kFnOpNeq;
                    break;
                }
                case zetasql::ASTBinaryExpression::Op::GT: {
                    op = node::FnOperator::kFnOpGt;
                    break;
                }
                case zetasql::ASTBinaryExpression::Op::LT: {
                    op = node::FnOperator::kFnOpLt;
                    break;
                }
                case zetasql::ASTBinaryExpression::Op::GE: {
                    op = node::FnOperator::kFnOpGe;
                    break;
                }
                case zetasql::ASTBinaryExpression::Op::LE: {
                    op = node::FnOperator::kFnOpLe;
                    break;
                }
                case zetasql::ASTBinaryExpression::Op::PLUS: {
                    op = node::FnOperator::kFnOpAdd;
                    break;
                }
                case zetasql::ASTBinaryExpression::Op::MINUS: {
                    op = node::FnOperator::kFnOpMinus;
                    break;
                }
                case zetasql::ASTBinaryExpression::Op::MULTIPLY: {
                    op = node::FnOperator::kFnOpMulti;
                    break;
                }
                case zetasql::ASTBinaryExpression::Op::DIVIDE: {
                    op = node::FnOperator::kFnOpFDiv;
                    break;
                }
                case zetasql::ASTBinaryExpression::Op::IDIVIDE: {
                    op = node::FnOperator::kFnOpDiv;
                    break;
                }
                case zetasql::ASTBinaryExpression::Op::LIKE: {
                    op = node::FnOperator::kFnOpLike;
                    break;
                }
                case zetasql::ASTBinaryExpression::Op::ILIKE: {
                    op = node::FnOperator::kFnOpILike;
                    break;
                }
                case zetasql::ASTBinaryExpression::Op::RLIKE: {
                    op = node::FnOperator::kFnOpRLike;
                    break;
                }
                case zetasql::ASTBinaryExpression::Op::MOD: {
                    op = node::FnOperator::kFnOpMod;
                    break;
                }
                case zetasql::ASTBinaryExpression::Op::XOR: {
                    op = node::FnOperator::kFnOpXor;
                    break;
                }
                case zetasql::ASTBinaryExpression::Op::BITWISE_AND: {
                    op = node::FnOperator::kFnOpBitwiseAnd;
                    break;
                }
                case zetasql::ASTBinaryExpression::Op::BITWISE_OR: {
                    op = node::FnOperator::kFnOpBitwiseOr;
                    break;
                }
                case zetasql::ASTBinaryExpression::Op::BITWISE_XOR: {
                    op = node::FnOperator::kFnOpBitwiseXor;
                    break;
                }
                default: {
                    status.msg = absl::StrCat("Unsupport binary operator: ", binary_expression->GetSQLForOperator());
                    status.code = common::kSqlAstError;
                    return status;
                }
            }
            node::ExprNode* out = node_manager->MakeBinaryExprNode(lhs, rhs, op);
            if (binary_expression->is_not()) {
                out = node_manager->MakeUnaryExprNode(out, node::FnOperator::kFnOpNot);
            }
            *output = out;
            return base::Status::OK();
        }
        case zetasql::AST_UNARY_EXPRESSION: {
            auto* unary_expression = ast_expression->GetAsOrDie<zetasql::ASTUnaryExpression>();
            node::ExprNode* operand = nullptr;
            node::FnOperator op;
            CHECK_STATUS(ConvertExprNode(unary_expression->operand(), node_manager, &operand))
            switch (unary_expression->op()) {
                case zetasql::ASTUnaryExpression::Op::MINUS: {
                    op = node::FnOperator::kFnOpMinus;
                    if (node::kExprPrimary == operand->expr_type_) {
                        node::ConstNode* const_node = dynamic_cast<node::ConstNode*>(operand);
                        CHECK_TRUE(const_node->ConvertNegative(), common::kSqlAstError, "Un-support negative ",
                                   node::DataTypeName(const_node->GetDataType()))
                        *output = const_node;
                        return base::Status::OK();
                    }
                    break;
                }
                case zetasql::ASTUnaryExpression::Op::NOT: {
                    op = node::FnOperator::kFnOpNot;
                    break;
                }
                case zetasql::ASTUnaryExpression::Op::PLUS: {
                    *output = operand;
                    return base::Status::OK();
                }
                case zetasql::ASTUnaryExpression::Op::BITWISE_NOT: {
                    op = node::FnOperator::kFnOpBitwiseNot;
                    break;
                }
                default: {
                    status.msg = absl::StrCat("Un-support unary operator: ", unary_expression->GetSQLForOperator());
                    status.code = common::kSqlAstError;
                    return status;
                }
            }
            *output = node_manager->MakeUnaryExprNode(operand, op);
            return base::Status::OK();
        }
        case zetasql::AST_AND_EXPR: {
            // TODO(chenjing): optimize AND expression from BinaryExprNode to AndExpr
            auto* and_expression = ast_expression->GetAsOrDie<zetasql::ASTAndExpr>();
            node::ExprNode* lhs = nullptr;
            CHECK_STATUS(ConvertExprNode(and_expression->conjuncts(0), node_manager, &lhs))
            CHECK_TRUE(nullptr != lhs, common::kSqlAstError, "Invalid AND expression")

            for (size_t i = 1; i < and_expression->conjuncts().size(); i++) {
                node::ExprNode* rhs = nullptr;
                CHECK_STATUS(ConvertExprNode(and_expression->conjuncts(i), node_manager, &rhs))
                CHECK_TRUE(nullptr != rhs, common::kSqlAstError, "Invalid AND expression")
                lhs = node_manager->MakeBinaryExprNode(lhs, rhs, node::FnOperator::kFnOpAnd);
            }
            *output = lhs;
            return base::Status();
        }

        case zetasql::AST_OR_EXPR: {
            // TODO(chenjing): optimize OR expression from BinaryExprNode to OrExpr
            auto* or_expression = ast_expression->GetAsOrDie<zetasql::ASTOrExpr>();
            node::ExprNode* lhs = nullptr;
            CHECK_STATUS(ConvertExprNode(or_expression->disjuncts()[0], node_manager, &lhs))
            CHECK_TRUE(nullptr != lhs, common::kSqlAstError, "Invalid OR expression")
            for (size_t i = 1; i < or_expression->disjuncts().size(); i++) {
                node::ExprNode* rhs = nullptr;
                CHECK_STATUS(ConvertExprNode(or_expression->disjuncts()[i], node_manager, &rhs))
                CHECK_TRUE(nullptr != rhs, common::kSqlAstError, "Invalid OR expression")
                lhs = node_manager->MakeBinaryExprNode(lhs, rhs, node::FnOperator::kFnOpOr);
            }
            *output = lhs;
            return base::Status();
        }
        case zetasql::AST_BETWEEN_EXPRESSION: {
            auto* between_expression = ast_expression->GetAsOrDie<zetasql::ASTBetweenExpression>();
            node::ExprNode* expr = nullptr;
            node::ExprNode* low = nullptr;
            node::ExprNode* high = nullptr;
            CHECK_STATUS(ConvertExprNode(between_expression->lhs(), node_manager, &expr))
            CHECK_STATUS(ConvertExprNode(between_expression->low(), node_manager, &low))
            CHECK_STATUS(ConvertExprNode(between_expression->high(), node_manager, &high))
            *output = node_manager->MakeBetweenExpr(expr, low, high, between_expression->is_not());

            return base::Status();
        }
        case zetasql::AST_FUNCTION_CALL: {
            auto* function_call = ast_expression->GetAsOrDie<zetasql::ASTFunctionCall>();
            CHECK_TRUE(false == function_call->HasModifiers(), common::kSqlAstError,
                       "Un-support Modifiers for function call")
            std::string function_name = "";
            CHECK_STATUS(AstPathExpressionToString(function_call->function(), &function_name))
            boost::to_lower(function_name);
            // Convert function call TYPE(value) to cast expression CAST(value as TYPE)
            node::DataType data_type;
            base::Status status = node::StringToDataType(function_name, &data_type);
            if (status.isOK() && 1 == function_call->arguments().size()) {
                node::ExprNode* arg;
                CHECK_STATUS(ConvertExprNode(function_call->arguments()[0], node_manager, &arg))
                *output = node_manager->MakeCastNode(data_type, arg);
            } else {
                node::ExprListNode* args = nullptr;
                CHECK_STATUS(ConvertExprNodeList(function_call->arguments(), node_manager, &args))
                *output = node_manager->MakeFuncNode(function_name, args, nullptr);
            }

            return base::Status::OK();
        }
        case zetasql::AST_ANALYTIC_FUNCTION_CALL: {
            auto* analytic_function_call = ast_expression->GetAsOrDie<zetasql::ASTAnalyticFunctionCall>();

            node::ExprNode* function_call = nullptr;
            node::WindowDefNode* over_winodw = nullptr;
            CHECK_STATUS(ConvertExprNode(analytic_function_call->function(), node_manager, &function_call));
            CHECK_STATUS(ConvertWindowSpecification(analytic_function_call->window_spec(), node_manager, &over_winodw))

            CHECK_TRUE(nullptr != function_call, common::kSqlAstError, "Invalid function call null expr")
            CHECK_TRUE(node::kExprCall == function_call->GetExprType(), common::kSqlAstError, "Non-support function "
                       "expression in function call", node::ExprTypeName(function_call->GetExprType()));
            dynamic_cast<node::CallExprNode*>(function_call)->SetOver(over_winodw);
            *output = function_call;
            return base::Status::OK();
        }
        case zetasql::AST_CAST_EXPRESSION: {
            const zetasql::ASTCastExpression* cast_expression =
                ast_expression->GetAsOrDie<zetasql::ASTCastExpression>();
            CHECK_TRUE(nullptr != cast_expression->expr(), common::kSqlAstError, "Invalid cast with null expr")
            CHECK_TRUE(nullptr != cast_expression->type(), common::kSqlAstError, "Invalid cast with null type")

            node::ExprNode* expr_node;
            CHECK_STATUS(ConvertExprNode(cast_expression->expr(), node_manager, &expr_node))
            node::TypeNode* tp = nullptr;
            CHECK_STATUS(ConvertASTType(cast_expression->type(), node_manager, &tp))

            // TODO(ace): cast from base type is not enough for type like array
            *output = node_manager->MakeCastNode(tp->base(), expr_node);
            return base::Status::OK();
        }
        case zetasql::AST_PARAMETER_EXPR: {
            const zetasql::ASTParameterExpr* parameter_expr = ast_expression->GetAsOrNull<zetasql::ASTParameterExpr>();
            CHECK_TRUE(nullptr != parameter_expr, common::kSqlAstError, "not an ASTParameterExpr")

            // Only support anonymous parameter (e.g, ?) so far.
            CHECK_TRUE(nullptr == parameter_expr->name(), common::kSqlAstError,
                       "Un-support Named Parameter Expression ", parameter_expr->name()->GetAsString());
            *output = node_manager->MakeParameterExpr(parameter_expr->position());
            return base::Status::OK();
        }
        case zetasql::AST_INT_LITERAL: {
            const zetasql::ASTIntLiteral* literal = ast_expression->GetAsOrDie<zetasql::ASTIntLiteral>();
            CHECK_TRUE(!literal->is_hex(), common::kSqlAstError, "Un-support hex integer literal: ", literal->image());

            int64_t int_value;
            CHECK_STATUS(ASTIntLiteralToNum(ast_expression, &int_value));

            if (!literal->is_long() && int_value <= INT32_MAX && int_value >= INT32_MIN) {
                *output = node_manager->MakeConstNode(static_cast<int32_t>(int_value));
            } else {
                *output = node_manager->MakeConstNode(int_value);
            }
            return base::Status::OK();
        }

        case zetasql::AST_STRING_LITERAL: {
            std::string str_value;
            CHECK_STATUS(AstStringLiteralToString(ast_expression, &str_value));
            *output = node_manager->MakeConstNode((str_value));
            return base::Status::OK();
        }

        case zetasql::AST_BOOLEAN_LITERAL: {
            const zetasql::ASTBooleanLiteral* literal = ast_expression->GetAsOrDie<zetasql::ASTBooleanLiteral>();
            bool bool_value;
            hybridse::codec::StringRef str(literal->image().size(), literal->image().data());
            bool is_null;
            hybridse::udf::v1::string_to_bool(&str, &bool_value, &is_null);
            CHECK_TRUE(!is_null, common::kSqlAstError, "Invalid bool literal: ", literal->image())
            *output = node_manager->MakeConstNode(bool_value);
            return base::Status::OK();
        }
        case zetasql::AST_FLOAT_LITERAL: {
            const zetasql::ASTFloatLiteral* literal = ast_expression->GetAsOrDie<zetasql::ASTFloatLiteral>();

            bool is_null;
            if (literal->is_float32()) {
                float float_value = 0.0;
                hybridse::codec::StringRef str(literal->image().size() - 1, literal->image().data());
                hybridse::udf::v1::string_to_float(&str, &float_value, &is_null);
                CHECK_TRUE(!is_null, common::kSqlAstError, "Invalid float literal: ", literal->image());
                *output = node_manager->MakeConstNode(float_value);
            } else {
                double double_value = 0.0;
                hybridse::codec::StringRef str(literal->image().size(), literal->image().data());
                hybridse::udf::v1::string_to_double(&str, &double_value, &is_null);
                CHECK_TRUE(!is_null, common::kSqlAstError, "Invalid double literal: ", literal->image());
                *output = node_manager->MakeConstNode(double_value);
            }
            return base::Status::OK();
        }
        case zetasql::AST_INTERVAL_LITERAL: {
            int64_t interval_value;
            node::DataType interval_unit;
            CHECK_STATUS(ASTIntervalLIteralToNum(ast_expression, &interval_value, &interval_unit))
            *output = node_manager->MakeConstNode(interval_value, interval_unit);
            return base::Status::OK();
        }

        case zetasql::AST_NULL_LITERAL: {
            // NULL literals are always treated as int64_t.  Literal coercion rules
            // may make the NULL change type.
            *output = node_manager->MakeConstNode();
            return base::Status::OK();
        }

        case zetasql::AST_DATE_OR_TIME_LITERAL:
        case zetasql::AST_NUMERIC_LITERAL:
        case zetasql::AST_BIGNUMERIC_LITERAL:
        case zetasql::AST_JSON_LITERAL:
        case zetasql::AST_BYTES_LITERAL: {
            FAIL_STATUS(common::kUnsupportSql, "Un-support literal expression for node kind ",
                        ast_expression->GetNodeKindString());
            break;
        }

        case zetasql::AST_IN_EXPRESSION: {
            auto in_expr = ast_expression->GetAsOrNull<zetasql::ASTInExpression>();
            CHECK_TRUE(in_expr != nullptr, common::kUnsupportSql, "not an ASTInExpression");

            node::ExprNode* lhs_expr = nullptr;
            CHECK_STATUS(ConvertExprNode(in_expr->lhs(), node_manager, &lhs_expr));

            bool is_not = in_expr->is_not();

            if (in_expr->in_list() != nullptr) {
                node::ExprListNode* in_list = nullptr;
                CHECK_STATUS(ConvertExprNodeList(in_expr->in_list()->list(), node_manager, &in_list));
                // can't handle large sized in list currently, just stop earlier
                CHECK_TRUE(in_list->GetChildNum() <= 1000, common::kPlanError,
                           absl::StrCat("Un-support: IN predicate size should not larger than 1000, but got ",
                                        in_list->GetChildNum()));
                *output = node_manager->MakeInExpr(lhs_expr, in_list, is_not);
            } else if (in_expr->query() != nullptr) {
                node::QueryNode* query_node = nullptr;
                CHECK_STATUS(ConvertQueryNode(in_expr->query(), node_manager, &query_node));
                node::QueryExpr* query_expr = node_manager->MakeQueryExprNode(query_node);
                *output = node_manager->MakeInExpr(lhs_expr, query_expr, is_not);
                // not support sub query in IN predicate, stop earlier
                CHECK_TRUE(false, common::kPlanError, "Un-support: IN predicate with sub query as in list");
            } else {
                // TODO(aceforeverd): support unnest expression
                return base::Status(common::kUnsupportSql,
                                    "Un-supported: IN predicate with unnest expression as in list");
            }
            return base::Status::OK();
        }

        case zetasql::AST_ESCAPED_EXPRESSION: {
            auto escaped_expr = ast_expression->GetAsOrNull<zetasql::ASTEscapedExpression>();
            CHECK_TRUE(escaped_expr != nullptr, common::kUnsupportSql, "not and ASTEscapedExpression");
            node::ExprNode* pattern = nullptr;
            CHECK_STATUS(ConvertExprNode(escaped_expr->expr(), node_manager, &pattern));
            node::ExprNode* escape = nullptr;
            CHECK_STATUS(ConvertExprNode(escaped_expr->escape(), node_manager, &escape));
            // limit: escape node is const string, string size can't >=2
            CHECK_TRUE(escape->GetExprType() == node::kExprPrimary &&
                           dynamic_cast<node::ConstNode*>(escape)->GetDataType() == node::DataType::kVarchar &&
                           dynamic_cast<node::ConstNode*>(escape)->GetAsString().size() <= 1,
                       common::kUnsupportSql, "escape value is not string or string size >= 2");

            *output = node_manager->MakeEscapeExpr(pattern, escape);
            return base::Status::OK();
        }

        case zetasql::AST_ARRAY_CONSTRUCTOR: {
            return ConvertGuard<zetasql::ASTArrayConstructor, node::ExprNode>(ast_expression, node_manager, output,
                                                                              ConvertArrayExpr);
        }

        default: {
            FAIL_STATUS(common::kUnsupportSql, "Unsupport ASTExpression ", ast_expression->GetNodeKindString())
        }
    }
    return status;
}
base::Status ConvertStatement(const zetasql::ASTStatement* statement, node::NodeManager* node_manager,
                              node::SqlNode** output) {
    switch (statement->node_kind()) {
        case zetasql::AST_QUERY_STATEMENT: {
            auto const query_stmt = statement->GetAsOrNull<zetasql::ASTQueryStatement>();
            CHECK_TRUE(query_stmt != nullptr, common::kSqlAstError, "not an ASTQueryStatement");
            node::QueryNode* query_node = nullptr;
            CHECK_STATUS(ConvertQueryNode(query_stmt->query(), node_manager, &query_node));
            if (query_stmt->config_clause() != nullptr) {
                auto options = std::make_shared<node::OptionsMap>();
                CHECK_STATUS(
                    ConvertAstOptionsListToMap(query_stmt->config_clause()->options_list(), node_manager, options));
                query_node->config_options_ = std::move(options);
            }
            *output = query_node;
            break;
        }
        case zetasql::AST_BEGIN_END_BLOCK: {
            auto const begin_end_block = statement->GetAsOrNull<zetasql::ASTBeginEndBlock>();
            CHECK_TRUE(begin_end_block != nullptr, common::kSqlAstError, "not and ASTBeginEndBlock");
            CHECK_TRUE(begin_end_block->statement_list().size() <= 1, common::kSqlAstError,
                       "Un-support multiple statements inside ASTBeginEndBlock");
            node::SqlNodeList* stmt_node_list = node_manager->MakeNodeList();
            for (const auto sub_stmt : begin_end_block->statement_list()) {
                CHECK_TRUE(sub_stmt->node_kind() == zetasql::AST_QUERY_STATEMENT, common::kSqlAstError,
                           "Un-support statement type inside ASTBeginEndBlock: ", sub_stmt->GetNodeKindString())
                node::SqlNode* stmt_node = nullptr;
                CHECK_STATUS(ConvertStatement(sub_stmt, node_manager, &stmt_node));
                stmt_node_list->PushBack(stmt_node);
            }
            *output = stmt_node_list;
            break;
        }
        case zetasql::AST_CREATE_TABLE_STATEMENT: {
            const zetasql::ASTCreateTableStatement* create_statement =
                statement->GetAsOrNull<zetasql::ASTCreateTableStatement>();
            CHECK_TRUE(nullptr != create_statement, common::kSqlAstError, "not an ASTCreateTableStatement")
            node::CreateStmt* create_node;
            CHECK_STATUS(ConvertCreateTableNode(create_statement, node_manager, &create_node))
            *output = create_node;
            break;
        }
        case zetasql::AST_CREATE_PROCEDURE_STATEMENT: {
            const zetasql::ASTCreateProcedureStatement* procedure_statement =
                statement->GetAsOrNull<zetasql::ASTCreateProcedureStatement>();
            CHECK_TRUE(nullptr != procedure_statement, common::kSqlAstError, "not an ASTCreateProcedureStatement");
            node::CreateSpStmt* create_sp_tree = nullptr;
            CHECK_STATUS(ConvertCreateProcedureNode(procedure_statement, node_manager, &create_sp_tree))
            *output = create_sp_tree;
            break;
        }
        case zetasql::AST_INSERT_STATEMENT: {
            const zetasql::ASTInsertStatement* insert_statement = statement->GetAsOrNull<zetasql::ASTInsertStatement>();
            CHECK_TRUE(nullptr != insert_statement, common::kSqlAstError, "not and ASTInsertStatement")
            node::InsertStmt* insert_node;
            CHECK_STATUS(ConvertInsertStatement(insert_statement, node_manager, &insert_node))
            *output = insert_node;
            break;
        }
        case zetasql::AST_SHOW_STATEMENT: {
            const zetasql::ASTShowStatement* show_statement = statement->GetAsOrNull<zetasql::ASTShowStatement>();
            CHECK_STATUS(convertShowStmt(show_statement, node_manager, output));
            break;
        }
        case zetasql::AST_CREATE_DATABASE_STATEMENT: {
            const zetasql::ASTCreateDatabaseStatement* create_database_statement =
                statement->GetAsOrNull<zetasql::ASTCreateDatabaseStatement>();
            CHECK_TRUE(nullptr != create_database_statement->name(), common::kSqlAstError,
                       "not an AST_CREATE_DATABASE_STATEMENT")

            std::vector<std::string> names;
            CHECK_STATUS(AstPathExpressionToStringList(create_database_statement->name(), names))
            CHECK_TRUE(1 == names.size(), common::kSqlAstError, "Invalid database path expression ",
                       create_database_statement->name()->ToIdentifierPathString())
            auto* node =
                dynamic_cast<node::CmdNode*>(node_manager->MakeCmdNode(node::CmdType::kCmdCreateDatabase, names[0]));
            node->SetIfNotExists(create_database_statement->is_if_not_exists());
            *output = node;
            break;
        }
        case zetasql::AST_DROP_FUNCTION_STATEMENT: {
            const zetasql::ASTDropFunctionStatement* drop_fun_statement =
                statement->GetAsOrNull<zetasql::ASTDropFunctionStatement>();
            CHECK_TRUE(nullptr != drop_fun_statement->name(), common::kSqlAstError,
                       "not an AST_DROP_FUNCTION_STATEMENT")

            std::vector<std::string> names;
            CHECK_STATUS(AstPathExpressionToStringList(drop_fun_statement->name(), names))
            CHECK_TRUE(1 == names.size(), common::kSqlAstError, "Invalid function path expression ",
                       drop_fun_statement->name()->ToIdentifierPathString())
            auto node =
                dynamic_cast<node::CmdNode*>(node_manager->MakeCmdNode(node::CmdType::kCmdDropFunction, names[0]));
            node->SetIfExists(drop_fun_statement->is_if_exists());
            *output = node;
            break;
        }
        case zetasql::AST_DESCRIBE_STATEMENT: {
            const zetasql::ASTDescribeStatement* describe_statement =
                statement->GetAsOrNull<zetasql::ASTDescribeStatement>();
            CHECK_TRUE(nullptr != describe_statement->name(), common::kSqlAstError,
                       "can't create plan for desc with null name")
            std::vector<std::string> names;
            CHECK_STATUS(AstPathExpressionToStringList(describe_statement->name(), names))
            *output =
                dynamic_cast<node::CmdNode*>(node_manager->MakeCmdNode(node::CmdType::kCmdDescTable, names));
            break;
        }
        case zetasql::AST_DROP_STATEMENT: {
            const zetasql::ASTDropStatement* drop_statement = statement->GetAsOrNull<zetasql::ASTDropStatement>();
            CHECK_TRUE(nullptr != drop_statement->name(), common::kSqlAstError, "not an ASTDropStatement")
            node::CmdNode* cmd_node;
            CHECK_STATUS(ConvertDropStatement(drop_statement, node_manager, &cmd_node));
            *output = cmd_node;
            break;
        }
        case zetasql::AST_EXPLAIN_STATEMENT: {
            const zetasql::ASTExplainStatement* explain_statement =
                statement->GetAsOrNull<zetasql::ASTExplainStatement>();
            CHECK_TRUE(nullptr != explain_statement, common::kSqlAstError, "not an ASTExplainStatement")
            CHECK_TRUE(nullptr != explain_statement->statement(), common::kSqlAstError, "can't explan null statement")
            CHECK_TRUE(zetasql::AST_QUERY_STATEMENT == explain_statement->statement()->node_kind(),
                       common::kSqlAstError, "Un-support explain statement with type ",
                       explain_statement->statement()->GetNodeKindString())
            node::SqlNode* query_node = nullptr;
            CHECK_STATUS(ConvertStatement(explain_statement->statement(), node_manager, &query_node))
            *output = node_manager->MakeExplainNode(dynamic_cast<node::QueryNode*>(query_node),
                                                    node::ExplainType::kExplainPhysical);
            break;
        }
        case zetasql::AST_CREATE_INDEX_STATEMENT: {
            const zetasql::ASTCreateIndexStatement* create_index_stmt =
                statement->GetAsOrNull<zetasql::ASTCreateIndexStatement>();
            node::CreateIndexNode* create_index_node;
            CHECK_STATUS(ConvertCreateIndexStatement(create_index_stmt, node_manager, &create_index_node))
            *output = create_index_node;
            break;
        }
        case zetasql::AST_USE_STATEMENT: {
            const auto use_stmt = statement->GetAsOrNull<zetasql::ASTUseStatement>();
            CHECK_TRUE(nullptr != use_stmt, common::kSqlAstError, "not an ASTUseStatement");
            const std::string db_name = use_stmt->db_name()->GetAsString();
            *output = node_manager->MakeCmdNode(node::CmdType::kCmdUseDatabase, db_name);
            break;
        }
        case zetasql::AST_EXIT_STATEMENT: {
            const auto exit_stmt = statement->GetAsOrNull<zetasql::ASTExitStatement>();
            CHECK_TRUE(nullptr != exit_stmt, common::kSqlAstError, "not an ASTExitStatement");
            *output = node_manager->MakeCmdNode(node::CmdType::kCmdExit);
            break;
        }
        case zetasql::AST_SYSTEM_VARIABLE_ASSIGNMENT: {
            /// support system variable setting and showing in OpenMLDB since v0.4.0
            /// non-support local variable setting in v0.4.0
            const auto ast_system_variable_assign = statement->GetAsOrNull<zetasql::ASTSystemVariableAssignment>();
            CHECK_TRUE(nullptr != ast_system_variable_assign, common::kSqlAstError,
                       "not an ASTSystemVariableAssignment");
            std::vector<std::string> path;
            CHECK_STATUS(AstPathExpressionToStringList(ast_system_variable_assign->system_variable()->path(), path));
            CHECK_TRUE(!path.empty(), common::kSqlAstError, "Non-support empty variable");

            node::ExprNode* value = nullptr;
            CHECK_STATUS(ConvertExprNode(ast_system_variable_assign->expression(), node_manager, &value));
            CHECK_TRUE(value->GetExprType() == node::kExprPrimary, common::kSqlAstError,
                       "Unsupported Set value other than const type");
            // System variable is session variable by default
            if (path.size() == 1) {
                *output = node_manager->MakeSetNode(node::VariableScope::kSessionSystemVariable,
                                                    path[0], dynamic_cast<node::ConstNode*>(value));
            } else if (path.size() == 2) {
                absl::string_view path_v(path[0]);
                if (absl::EqualsIgnoreCase(path_v, "global")) {
                    *output = node_manager->MakeSetNode(node::VariableScope::kGlobalSystemVariable, path[1],
                                                        dynamic_cast<node::ConstNode*>(value));
                } else if (absl::EqualsIgnoreCase(path_v, "session")) {
                    *output = node_manager->MakeSetNode(node::VariableScope::kSessionSystemVariable, path[1],
                                                        dynamic_cast<node::ConstNode*>(value));
                } else {
                    FAIL_STATUS(common::kSqlAstError, "Non-support system variable under ", path[0],
                                " scope, try @@global or @@session scope");
                }
            } else {
                FAIL_STATUS(common::kSqlAstError,
                            "Non-support system variable with more than 2 path "
                            "levels. Try @@global.var_name or @@session.var_name or @@var_name");
            }
            break;
        }
        case zetasql::AST_SCOPED_VARIABLE_ASSIGNMENT: {
            auto stmt = statement->GetAsOrNull<zetasql::ASTScopedVariableAssignment>();
            CHECK_TRUE(stmt != nullptr, common::kSqlAstError, "not an ASTScopedVariableAssignment");
            node::ExprNode* value = nullptr;
            CHECK_STATUS(ConvertExprNode(stmt->expression(), node_manager, &value));
            const node::ConstNode* const_value = dynamic_cast<node::ConstNode*>(value);
            CHECK_TRUE(const_value != nullptr && const_value->GetExprType() == node::kExprPrimary, common::kSqlAstError,
                       "Unsupported Set value other than const type");

            const std::string& identifier = stmt->variable()->GetAsString();
            auto scope = node::VariableScope::kSessionSystemVariable;
            if (stmt->scope() == zetasql::ASTScopedVariableAssignment::Scope::GLOBAL) {
                scope = node::VariableScope::kGlobalSystemVariable;
            }
            *output = node_manager->MakeSetNode(scope, identifier, const_value);
            break;
        }
        case zetasql::AST_LOAD_DATA_STATEMENT: {
            const auto load_data_stmt = statement->GetAsOrNull<zetasql::ASTLoadDataStatement>();
            CHECK_TRUE(load_data_stmt != nullptr, common::kSqlAstError, "not an ASTLoadDataStatement");
            std::string file_name = load_data_stmt->in_file()->string_value();

            CHECK_TRUE(load_data_stmt->table_name()->num_names() <= 2, common::kSqlAstError,
                       "unsupported size of table path");
            std::vector<std::string> table_path;
            CHECK_STATUS(AstPathExpressionToStringList(load_data_stmt->table_name(), table_path));
            std::string table = table_path.back();
            std::string db;
            if (table_path.size() == 2) {
                db = table_path[0];
            }

            auto options = std::make_shared<node::OptionsMap>();
            if (load_data_stmt->options_list() != nullptr) {
                CHECK_STATUS(ConvertAstOptionsListToMap(load_data_stmt->options_list(), node_manager, options));
            }
            auto config_options = std::make_shared<node::OptionsMap>();
            if (load_data_stmt->opt_config() != nullptr) {
                CHECK_STATUS(ConvertAstOptionsListToMap(load_data_stmt->opt_config()->options_list(), node_manager,
                                                        config_options));
            }
            *output =
                node_manager->MakeLoadDataNode(file_name, db, table, options, config_options);
            break;
        }
        case zetasql::AST_DEPLOY_STATEMENT: {
            const auto ast_deploy_stmt = statement->GetAsOrNull<zetasql::ASTDeployStatement>();
            CHECK_TRUE(ast_deploy_stmt != nullptr, common::kSqlAstError, "not an ASTDeployStatement");
            node::SqlNode* deploy_stmt = nullptr;
            CHECK_STATUS(ConvertStatement(ast_deploy_stmt->stmt(), node_manager, &deploy_stmt));

            auto options = std::make_shared<node::OptionsMap>();
            if (ast_deploy_stmt->options_list() != nullptr) {
                CHECK_STATUS(ConvertAstOptionsListToMap(ast_deploy_stmt->options_list(), node_manager, options, true));
            }
            *output = node_manager->MakeDeployStmt(ast_deploy_stmt->name()->GetAsString(), deploy_stmt,
                                                   ast_deploy_stmt->UnparseStmt(), options,
                                                   ast_deploy_stmt->is_if_not_exists());
            break;
        }
        case zetasql::AST_SELECT_INTO_STATEMENT: {
            const auto ast_select_into_stmt = statement->GetAsOrNull<zetasql::ASTSelectIntoStatement>();
            CHECK_TRUE(ast_select_into_stmt != nullptr, common::kSqlAstError, "not an ASTSelectIntoStatement");
            node::QueryNode* query = nullptr;
            CHECK_STATUS(ConvertQueryNode(ast_select_into_stmt->query(), node_manager, &query));

            std::string out_file = ast_select_into_stmt->out_file()->string_value();

            auto options = std::make_shared<node::OptionsMap>();
            if (ast_select_into_stmt->options_list() != nullptr) {
                CHECK_STATUS(ConvertAstOptionsListToMap(ast_select_into_stmt->options_list(), node_manager, options));
            }
            auto config_options = std::make_shared<node::OptionsMap>();
            if (ast_select_into_stmt->opt_config() != nullptr) {
                CHECK_STATUS(ConvertAstOptionsListToMap(ast_select_into_stmt->opt_config()->options_list(),
                                                        node_manager, config_options));
            }
            *output = node_manager->MakeSelectIntoNode(query, ast_select_into_stmt->UnparseQuery(), out_file, options,
                                                       config_options);
            break;
        }
        case zetasql::AST_STOP_STATEMENT: {
            std::vector<absl::string_view> targets;
            auto stop_stmt = statement->GetAsOrNull<zetasql::ASTStopStatement>();
            CHECK_TRUE(stop_stmt != nullptr, common::kSqlAstError, "not an ASTStopStatement");
            auto id = stop_stmt->identifier()->GetAsStringView();
            if (absl::EqualsIgnoreCase(id, "job")) {
                CHECK_STATUS(ConvertTargetName(stop_stmt->target_name(), targets));
                CHECK_TRUE(targets.size() == 1, common::kSqlAstError, "unsupported stop job with path name >= 2");
                *output = node_manager->MakeCmdNode(node::CmdType::kCmdStopJob,
                                                    std::string(targets.front().data(), targets.front().size()));
            } else {
                FAIL_STATUS(common::kSqlAstError, "unsupported type for stop statement: ", id);
            }
            break;
        }
        case zetasql::AST_DELETE_STATEMENT: {
            auto delete_stmt = statement->GetAsOrNull<zetasql::ASTDeleteStatement>();
            CHECK_TRUE(delete_stmt != nullptr, common::kSqlAstError, "not an ASTDeleteStatement");
            node::DeleteNode* delete_node = nullptr;
            CHECK_STATUS(ConvertDeleteNode(delete_stmt, node_manager, &delete_node));
            *output = delete_node;
            break;
        }
        case zetasql::AST_CREATE_FUNCTION_STATEMENT: {
            const auto ast_create_function_stmt = statement->GetAsOrNull<zetasql::ASTCreateFunctionStatement>();
            CHECK_TRUE(ast_create_function_stmt != nullptr, common::kSqlAstError, "not an ASTCreateFunctionStatement");
            node::CreateFunctionNode* create_fun_node = nullptr;
            CHECK_STATUS(ConvertCreateFunctionNode(ast_create_function_stmt, node_manager, &create_fun_node));
            *output = create_fun_node;
            break;
        }
        case zetasql::AST_ALTER_TABLE_STATEMENT: {
            CHECK_STATUS(
                ConvertGuard<zetasql::ASTAlterTableStatement>(statement, node_manager, output, ConvertAlterTableStmt));
            break;
        }
        default: {
            FAIL_STATUS(common::kSqlAstError, "Un-support statement type: ", statement->GetNodeKindString());
        }
    }
    return base::Status::OK();
}

base::Status ConvertOrderBy(const zetasql::ASTOrderBy* order_by, node::NodeManager* node_manager,
                            node::OrderByNode** output) {
    if (nullptr == order_by) {
        *output = nullptr;
        return base::Status::OK();
    }
    auto ordering_expressions = node_manager->MakeExprList();
    std::vector<bool> is_asc_list;
    for (auto ordering_expression : order_by->ordering_expressions()) {
        node::ExprNode* expr = nullptr;
        CHECK_STATUS(ConvertExprNode(ordering_expression->expression(), node_manager, &expr))
        ordering_expressions->AddChild(node_manager->MakeOrderExpression(expr, !ordering_expression->descending()));
    }

    *output = node_manager->MakeOrderByNode(ordering_expressions);
    return base::Status::OK();
}
base::Status ConvertDotStart(const zetasql::ASTDotStar* dot_start_expression, node::NodeManager* node_manager,
                             node::ExprNode** output) {
    base::Status status;
    if (nullptr == dot_start_expression) {
        *output = nullptr;
        return base::Status::OK();
    }
    if (nullptr == dot_start_expression->expr()) {
        *output = node_manager->MakeAllNode("");
        return base::Status::OK();
    }
    switch (dot_start_expression->expr()->node_kind()) {
        case zetasql::AST_PATH_EXPRESSION: {
            auto path_expression = dot_start_expression->expr()->GetAsOrDie<zetasql::ASTPathExpression>();
            int num_names = path_expression->num_names();
            if (1 == num_names) {
                *output = node_manager->MakeAllNode(path_expression->first_name()->GetAsString(), "");
            } else if (2 == num_names) {
                *output = node_manager->MakeAllNode(path_expression->name(0)->GetAsString(),
                                                    path_expression->name(1)->GetAsString());
            } else {
                status.code = common::kSqlAstError;
                status.msg = "Invalid column path expression " + path_expression->ToIdentifierPathString();
                return status;
            }
            break;
        }
        default: {
            status.code = common::kSqlAstError;
            status.msg = "Un-support dot star expression " + dot_start_expression->expr()->GetNodeKindString();
            return status;
        }
    }
    return base::Status::OK();
}
base::Status ConvertExprNodeList(const absl::Span<const zetasql::ASTExpression* const>& expression_list,
                                 node::NodeManager* node_manager, node::ExprListNode** output) {
    if (expression_list.empty()) {
        *output = nullptr;
        return base::Status::OK();
    }
    auto expr_list = node_manager->MakeExprList();
    for (auto expression : expression_list) {
        node::ExprNode* expr = nullptr;
        CHECK_STATUS(ConvertExprNode(expression, node_manager, &expr))
        expr_list->AddChild(expr);
    }
    *output = expr_list;
    return base::Status::OK();
}
base::Status ConvertFrameBound(const zetasql::ASTWindowFrameExpr* window_frame_expr, node::NodeManager* node_manager,
                               node::FrameBound** output) {
    if (nullptr == window_frame_expr) {
        *output = nullptr;
        return base::Status::OK();
    }
    base::Status status;
    node::ExprNode* expr = nullptr;
    node::BoundType bound_type = node::BoundType::kCurrent;
    switch (window_frame_expr->boundary_type()) {
        case zetasql::ASTWindowFrameExpr::BoundaryType::CURRENT_ROW: {
            bound_type = node::BoundType::kCurrent;
            break;
        }
        case zetasql::ASTWindowFrameExpr::BoundaryType::OFFSET_PRECEDING: {
            bound_type =
                window_frame_expr->is_open_boundary() ? node::BoundType::kOpenPreceding : node::BoundType::kPreceding;
            break;
        }
        case zetasql::ASTWindowFrameExpr::BoundaryType::UNBOUNDED_PRECEDING: {
            bound_type = node::BoundType::kPrecedingUnbound;
            break;
        }
        case zetasql::ASTWindowFrameExpr::BoundaryType::OFFSET_FOLLOWING: {
            bound_type =
                window_frame_expr->is_open_boundary() ? node::BoundType::kOpenFollowing : node::BoundType::kFollowing;
            break;
        }
        case zetasql::ASTWindowFrameExpr::BoundaryType::UNBOUNDED_FOLLOWING: {
            bound_type = node::BoundType::kFollowingUnbound;
            break;
        }
        default: {
            status.msg = "Un-support boundary type " + window_frame_expr->GetBoundaryTypeString();
            status.code = common::kSqlAstError;
            return status;
        }
    }
    CHECK_STATUS(ConvertExprNode(window_frame_expr->expression(), node_manager, &expr));
    if (nullptr == expr) {
        *output = dynamic_cast<node::FrameBound*>(node_manager->MakeFrameBound(bound_type));
    } else {
        *output = dynamic_cast<node::FrameBound*>(node_manager->MakeFrameBound(bound_type, expr));
    }
    return base::Status::OK();
}
base::Status ConvertFrameNode(const zetasql::ASTWindowFrame* window_frame, node::NodeManager* node_manager,
                              node::FrameNode** output) {
    if (nullptr == window_frame) {
        *output = nullptr;
        return base::Status::OK();
    }
    base::Status status;
    node::FrameType frame_type;
    switch (window_frame->frame_unit()) {
        case zetasql::ASTWindowFrame::FrameUnit::ROWS: {
            frame_type = node::kFrameRows;
            break;
        }
        case zetasql::ASTWindowFrame::FrameUnit::RANGE: {
            frame_type = node::kFrameRange;
            break;
        }
        case zetasql::ASTWindowFrame::FrameUnit::ROWS_RANGE: {
            frame_type = node::kFrameRowsRange;
            break;
        }
        default: {
            status.msg = "Un-support frame type " + window_frame->GetFrameUnitString();
            status.code = common::kSqlAstError;
            return status;
        }
    }
    node::FrameBound* start = nullptr;
    node::FrameBound* end = nullptr;
    CHECK_TRUE(nullptr != window_frame->start_expr(), common::kSqlAstError, "Un-support window frame with null start")
    CHECK_TRUE(nullptr != window_frame->end_expr(), common::kSqlAstError, "Un-support window frame with null end")
    CHECK_STATUS(ConvertFrameBound(window_frame->start_expr(), node_manager, &start))
    CHECK_STATUS(ConvertFrameBound(window_frame->end_expr(), node_manager, &end))
    node::ExprNode* frame_max_size = nullptr;
    if (nullptr != window_frame->max_size()) {
        CHECK_STATUS(ConvertExprNode(window_frame->max_size()->max_size(), node_manager, &frame_max_size))
    }
    auto* frame_ext = node_manager->MakeFrameExtent(start, end);
    CHECK_TRUE(frame_ext->Valid(), common::kSqlAstError,
               "The lower bound of a window frame must be less than or equal to the upper bound");
    *output = node_manager->MakeFrameNode(frame_type, frame_ext, frame_max_size);
    return base::Status::OK();
}
base::Status ConvertWindowDefinition(const zetasql::ASTWindowDefinition* window_definition,
                                     node::NodeManager* node_manager, node::WindowDefNode** output) {
    if (nullptr == window_definition) {
        *output = nullptr;
        return base::Status::OK();
    }
    CHECK_STATUS(ConvertWindowSpecification(window_definition->window_spec(), node_manager, output));

    if (nullptr != output && nullptr != window_definition->name()) {
        (*output)->SetName(window_definition->name()->GetAsString());
    }
    return base::Status::OK();
}
base::Status ConvertWindowSpecification(const zetasql::ASTWindowSpecification* window_spec,
                                        node::NodeManager* node_manager, node::WindowDefNode** output) {
    node::ExprListNode* partition_by = nullptr;
    node::OrderByNode* order_by = nullptr;
    node::FrameNode* frame_node = nullptr;
    if (nullptr != window_spec->partition_by()) {
        CHECK_STATUS(
            ConvertExprNodeList(window_spec->partition_by()->partitioning_expressions(), node_manager, &partition_by))
    }
    if (nullptr != window_spec->order_by()) {
        CHECK_STATUS(ConvertOrderBy(window_spec->order_by(), node_manager, &order_by))
    }
    if (nullptr != window_spec->window_frame()) {
        CHECK_STATUS(ConvertFrameNode(window_spec->window_frame(), node_manager, &frame_node));
        CHECK_TRUE(frame_node != nullptr, common::kPlanError);
        frame_node->exclude_current_row_ = window_spec->is_exclude_current_row();
    }

    node::SqlNodeList* union_tables = nullptr;

    if (nullptr != window_spec->union_table_references()) {
        union_tables = node_manager->MakeNodeList();
        for (auto table_reference : window_spec->union_table_references()->table_references()) {
            node::TableRefNode* union_table = nullptr;
            CHECK_STATUS(ConvertTableExpressionNode(table_reference, node_manager, &union_table))
            union_tables->PushBack(union_table);
        }
    }
    *output = dynamic_cast<node::WindowDefNode*>(node_manager->MakeWindowDefNode(
        union_tables, partition_by, order_by, frame_node, window_spec->is_exclude_current_time(),
        window_spec->is_instance_not_in_window()));
    if (nullptr != window_spec->base_window_name()) {
        (*output)->SetName(window_spec->base_window_name()->GetAsString());
    }
    return base::Status::OK();
}
base::Status ConvertSelectList(const zetasql::ASTSelectList* select_list, node::NodeManager* node_manager,
                               node::SqlNodeList** output) {
    base::Status status;
    if (nullptr == select_list) {
        *output = nullptr;
        return base::Status::OK();
    }
    *output = node_manager->MakeNodeList();
    for (auto select_column : select_list->columns()) {
        std::string project_name;
        node::ExprNode* project_expr = nullptr;
        CHECK_STATUS(ConvertExprNode(select_column->expression(), node_manager, &project_expr))
        project_name = nullptr != select_column->alias() ? select_column->alias()->GetAsString() : "";
        (*output)->PushBack(node_manager->MakeResTargetNode(project_expr, project_name));
    }
    return base::Status::OK();
}
base::Status ConvertTableExpressionNode(const zetasql::ASTTableExpression* root, node::NodeManager* node_manager,
                                        node::TableRefNode** output) {
    base::Status status;
    if (nullptr == root) {
        *output = nullptr;
        return status;
    }
    switch (root->node_kind()) {
        case zetasql::AST_TABLE_PATH_EXPRESSION: {
            auto table_path_expression = root->GetAsOrDie<zetasql::ASTTablePathExpression>();

            CHECK_TRUE(nullptr == table_path_expression->pivot_clause(), common::kSqlAstError,
                       "Un-support pivot clause")
            CHECK_TRUE(nullptr == table_path_expression->unpivot_clause(), common::kSqlAstError,
                       "Un-support unpivot clause")
            CHECK_TRUE(nullptr == table_path_expression->for_system_time(), common::kSqlAstError,
                       "Un-support system time")
            CHECK_TRUE(nullptr == table_path_expression->with_offset(), common::kSqlAstError,
                       "Un-support scan WITH OFFSET")
            CHECK_TRUE(nullptr == table_path_expression->sample_clause(), common::kSqlAstError,
                       "Un-support tablesample clause")
            CHECK_TRUE(nullptr == table_path_expression->hint(), common::kSqlAstError, "Un-support hint")

            std::vector<std::string> names;
            CHECK_STATUS(AstPathExpressionToStringList(table_path_expression->path_expr(), names))

            CHECK_TRUE(names.size() >= 1 && names.size() <= 2, common::kSqlAstError, "Invalid table path expression ",
                       table_path_expression->path_expr()->ToIdentifierPathString())
            std::string alias_name =
                nullptr != table_path_expression->alias() ? table_path_expression->alias()->GetAsString() : "";
            if (names.size() == 1) {
                *output = node_manager->MakeTableNode(names[0], alias_name);
            } else {
                *output = node_manager->MakeTableNode(names[0], names[1], alias_name);
            }
            break;
        }
        case zetasql::AST_JOIN: {
            auto join = root->GetAsOrDie<zetasql::ASTJoin>();
            CHECK_TRUE(nullptr == join->hint(), common::kSqlAstError, "Un-support hint with join")

            CHECK_TRUE(zetasql::ASTJoin::JoinHint::NO_JOIN_HINT == join->join_hint(), common::kSqlAstError,
                       "Un-support join hint with join ", join->GetSQLForJoinHint())
            CHECK_TRUE(nullptr == join->using_clause(), common::kSqlAstError, "Un-support USING clause with join ")
            CHECK_TRUE(false == join->natural(), common::kSqlAstError, "Un-support natural with join ")
            node::TableRefNode* left = nullptr;
            node::TableRefNode* right = nullptr;
            node::OrderByNode* order_by = nullptr;
            node::ExprNode* condition = nullptr;
            node::JoinType join_type = node::JoinType::kJoinTypeInner;
            CHECK_STATUS(ConvertTableExpressionNode(join->lhs(), node_manager, &left))
            CHECK_STATUS(ConvertTableExpressionNode(join->rhs(), node_manager, &right))
            CHECK_STATUS(ConvertOrderBy(join->order_by(), node_manager, &order_by))
            if (nullptr != join->on_clause()) {
                CHECK_STATUS(ConvertExprNode(join->on_clause()->expression(), node_manager, &condition))
            }
            switch (join->join_type()) {
                case zetasql::ASTJoin::JoinType::FULL: {
                    join_type = node::JoinType::kJoinTypeFull;
                    break;
                }
                case zetasql::ASTJoin::JoinType::LEFT: {
                    join_type = node::JoinType::kJoinTypeLeft;
                    break;
                }
                case zetasql::ASTJoin::JoinType::RIGHT: {
                    join_type = node::JoinType::kJoinTypeRight;
                    break;
                }
                case zetasql::ASTJoin::JoinType::LAST: {
                    join_type = node::JoinType::kJoinTypeLast;
                    break;
                }
                case zetasql::ASTJoin::JoinType::INNER: {
                    join_type = node::JoinType::kJoinTypeInner;
                    break;
                }
                case zetasql::ASTJoin::JoinType::COMMA: {
                    join_type = node::JoinType::kJoinTypeComma;
                    break;
                }
                default: {
                    status.msg = "Un-support join type " + join->GetSQLForJoinType();
                    status.code = common::kSqlAstError;
                    *output = nullptr;
                    return status;
                }
            }
            std::string alias_name = nullptr != join->alias() ? join->alias()->GetAsString() : "";
            if (node::kJoinTypeLast == join_type) {
                *output = node_manager->MakeLastJoinNode(left, right, order_by, condition, alias_name);
            } else {
                *output = node_manager->MakeJoinNode(left, right, join_type, condition, alias_name);
            }
            break;
        }
        case zetasql::AST_TABLE_SUBQUERY: {
            auto table_subquery = root->GetAsOrDie<zetasql::ASTTableSubquery>();
            std::string alias_name = nullptr == table_subquery->alias() ? "" : table_subquery->alias()->GetAsString();
            node::QueryNode* subquery = nullptr;
            CHECK_STATUS(ConvertQueryNode(table_subquery->subquery(), node_manager, &subquery))
            *output = node_manager->MakeQueryRefNode(subquery, alias_name);
            break;
        }
        default: {
            status.msg = "fail to convert table expression, unrecognized type " + root->GetNodeKindString();
            status.code = common::kSqlAstError;
            LOG(WARNING) << status;
            return status;
        }
    }

    return base::Status::OK();
}
base::Status ConvertGroupItems(const zetasql::ASTGroupBy* group_by, node::NodeManager* node_manager,
                               node::ExprListNode** output) {
    if (nullptr == group_by) {
        *output = nullptr;
        return base::Status::OK();
    }
    *output = node_manager->MakeExprList();
    for (auto grouping_item : group_by->grouping_items()) {
        node::ExprNode* group_expr = nullptr;
        CHECK_STATUS(ConvertExprNode(grouping_item->expression(), node_manager, &group_expr))
        (*output)->AddChild(group_expr);
    }
    return base::Status::OK();
}
base::Status ConvertWindowClause(const zetasql::ASTWindowClause* window_clause, node::NodeManager* node_manager,
                                 node::SqlNodeList** output) {
    base::Status status;
    if (nullptr == window_clause) {
        *output = nullptr;
        return base::Status::OK();
    }
    *output = node_manager->MakeNodeList();
    for (auto window : window_clause->windows()) {
        std::string project_name;
        node::WindowDefNode* window_def = nullptr;
        CHECK_STATUS(ConvertWindowDefinition(window, node_manager, &window_def))
        (*output)->PushBack(window_def);
    }
    return base::Status::OK();
}
base::Status ConvertLimitOffsetNode(const zetasql::ASTLimitOffset* limit_offset, node::NodeManager* node_manager,
                                    node::SqlNode** output) {
    base::Status status;
    if (nullptr == limit_offset) {
        *output = nullptr;
        return base::Status::OK();
    }

    CHECK_TRUE(nullptr == limit_offset->offset(), common::kSqlAstError, "Un-support OFFSET")
    CHECK_TRUE(nullptr != limit_offset->limit(), common::kSqlAstError, "Un-support LIMIT with null expression")

    node::ExprNode* limit = nullptr;
    CHECK_STATUS(ConvertExprNode(limit_offset->limit(), node_manager, &limit))
    CHECK_TRUE(node::kExprPrimary == limit->GetExprType(), common::kSqlAstError,
               "Un-support LIMIT with expression type ", limit_offset->GetNodeKindString())
    node::ConstNode* value = dynamic_cast<node::ConstNode*>(limit);
    switch (value->GetDataType()) {
        case node::kInt16:
        case node::kInt32:
        case node::kInt64: {
            *output = node_manager->MakeLimitNode(value->GetAsInt64());
            return base::Status::OK();
        }
        default: {
            status.code = common::kSqlAstError;
            status.msg = "Un-support LIMIT with expression type " + limit_offset->GetNodeKindString();
            return status;
        }
    }
}
base::Status ConvertQueryNode(const zetasql::ASTQuery* root, node::NodeManager* node_manager,
                              node::QueryNode** output) {
    base::Status status;
    if (nullptr == root) {
        *output = nullptr;
        return base::Status::OK();
    }

    const zetasql::ASTQueryExpression* query_expression = root->query_expr();
    node::OrderByNode* order_by = nullptr;
    CHECK_STATUS(ConvertOrderBy(root->order_by(), node_manager, &order_by));
    node::SqlNode* limit = nullptr;
    CHECK_STATUS(ConvertLimitOffsetNode(root->limit_offset(), node_manager, &limit));

    node::QueryNode* query_node = nullptr;
    CHECK_STATUS(ConvertQueryExpr(query_expression, node_manager, &query_node));

    if (root->with_clause() != nullptr) {
        auto* list = node_manager->MakeList<node::WithClauseEntry>();
        CHECK_STATUS(ConvertWithClause(root->with_clause(), node_manager, list));
        auto span = absl::MakeSpan(list->data_);
        query_node->SetWithClauses(span);
    }

    // HACK: set SelectQueryNode's limit and order
    //   UnionQueryNode do not match zetasql's union stmt
    if (query_node->query_type_ == node::kQuerySelect) {
        auto select_query_node = static_cast<node::SelectQueryNode*>(query_node);
        select_query_node->SetLimit(limit);
        select_query_node->SetOrder(order_by);
    }
    *output = query_node;
    return base::Status::OK();
}

base::Status ConvertQueryExpr(const zetasql::ASTQueryExpression* query_expression, node::NodeManager* node_manager,
                              node::QueryNode** output) {
    switch (query_expression->node_kind()) {
        case zetasql::AST_SELECT: {
            auto select_query = query_expression->GetAsOrNull<zetasql::ASTSelect>();
            bool is_distinct = false;
            node::SqlNodeList* select_list_ptr = nullptr;
            node::SqlNodeList* tableref_list_ptr = nullptr;
            node::ExprNode* where_expr = nullptr;
            node::ExprListNode* group_expr_list = nullptr;
            node::ExprNode* having_expr = nullptr;
            node::SqlNodeList* window_list_ptr = nullptr;
            node::TableRefNode* table_ref_node = nullptr;
            CHECK_STATUS(ConvertSelectList(select_query->select_list(), node_manager, &select_list_ptr));
            if (nullptr != select_query->from_clause()) {
                CHECK_STATUS(ConvertTableExpressionNode(select_query->from_clause()->table_expression(), node_manager,
                                                        &table_ref_node))
                if (nullptr != table_ref_node) {
                    tableref_list_ptr = node_manager->MakeNodeList();
                    tableref_list_ptr->PushBack(table_ref_node);
                }
            }
            if (nullptr != select_query->where_clause()) {
                CHECK_STATUS(ConvertExprNode(select_query->where_clause()->expression(), node_manager, &where_expr))
            }

            if (nullptr != select_query->group_by()) {
                CHECK_STATUS(ConvertGroupItems(select_query->group_by(), node_manager, &group_expr_list))
            }

            if (nullptr != select_query->having()) {
                CHECK_STATUS(ConvertExprNode(select_query->having()->expression(), node_manager, &having_expr))
            }

            if (nullptr != select_query->window_clause()) {
                CHECK_STATUS(ConvertWindowClause(select_query->window_clause(), node_manager, &window_list_ptr))
            }
            *output =
                node_manager->MakeSelectQueryNode(is_distinct, select_list_ptr, tableref_list_ptr, where_expr,
                                                  group_expr_list, having_expr, nullptr, window_list_ptr, nullptr);
            return base::Status::OK();
        }
        case zetasql::AST_SET_OPERATION: {
            const auto set_op = query_expression->GetAsOrNull<zetasql::ASTSetOperation>();
            CHECK_TRUE(set_op != nullptr, common::kSqlAstError, "not an ASTSetOperation");
            switch (set_op->op_type()) {
                case zetasql::ASTSetOperation::OperationType::UNION: {
                    CHECK_TRUE(set_op->inputs().size() >= 2, common::kSqlAstError,
                               "Union Set Operation have inputs size less than 2");
                    bool is_distinct = set_op->distinct();
                    node::QueryNode* left = nullptr;
                    CHECK_STATUS(ConvertQueryExpr(set_op->inputs().at(0), node_manager, &left));

                    for (size_t i = 1; i < set_op->inputs().size(); ++i) {
                        auto input = set_op->inputs().at(i);
                        node::QueryNode* expr_node = nullptr;
                        // TODO(aceforeverd): support set operation
                        CHECK_STATUS(ConvertQueryExpr(input, node_manager, &expr_node));
                        left = node_manager->MakeUnionQueryNode(left, expr_node, !is_distinct);
                    }

                    *output = left;
                    return base::Status::OK();
                }
                default: {
                    return base::Status(common::kSqlAstError,
                                        absl::StrCat("Un-support set operation: ", set_op->GetSQLForOperation()));
                }
            }
        }
        default: {
            // NOTE: code basically won't reach here unless inner error
            return base::Status(common::kSqlAstError,
                                absl::StrCat("can not create query plan node with invalid query type ",
                                             query_expression->GetNodeKindString()));
        }
    }
}

// ASTCreateTableStatement
//   (table_name, ASTTableElementList, ASTOptionsList, not_exist, like_clause)
//     -> (ASTTableElementList -> SqlNodeList)
//     -> (ASTOptionsList -> SqlNodeList)
//     -> CreateStmt
base::Status ConvertCreateTableNode(const zetasql::ASTCreateTableStatement* ast_create_stmt,
                                    node::NodeManager* node_manager, node::CreateStmt** output) {
    CHECK_TRUE(ast_create_stmt != nullptr, common::kOk, "ASTCreateTableStatement is null");

    bool if_not_exist = ast_create_stmt->is_if_not_exists();
    std::string table_name = "";
    std::string db_name = "";
    std::vector<std::string> names;
    CHECK_STATUS(AstPathExpressionToStringList(ast_create_stmt->name(), names));
    table_name = names.back();
    if (names.size() == 2) {
        db_name = names[0];
    }
    auto column_list = ast_create_stmt->table_element_list();
    node::SqlNodeList* column_desc_list = nullptr;

    if (column_list != nullptr) {
        column_desc_list = node_manager->MakeNodeList();
        for (auto ele : column_list->elements()) {
            node::SqlNode* column = nullptr;
            CHECK_STATUS(ConvertTableElement(ele, node_manager, &column));
            column_desc_list->PushBack(column);
        }
    }

    std::shared_ptr<node::CreateTableLikeClause> like_clause = nullptr;
    if (ast_create_stmt->like_table_clause() != nullptr) {
        like_clause = std::make_shared<node::CreateTableLikeClause>();
        // handling `LIKE PARQUET '...'`
        switch (ast_create_stmt->like_table_clause()->kind()) {
            case zetasql::ASTLikeTableClause::TableKind::PARQUET: {
                like_clause->kind_ = node::CreateTableLikeClause::PARQUET;
                break;
            }
            case zetasql::ASTLikeTableClause::TableKind::HIVE: {
                like_clause->kind_ = node::CreateTableLikeClause::HIVE;
                break;
            }
            default: {
                FAIL_STATUS(common::kSqlAstError, "unknown like clause kind for create table");
            }
        }

        like_clause->path_ = ast_create_stmt->like_table_clause()->path()->string_value();
    }

    const auto ast_option_list = ast_create_stmt->options_list();
    node::SqlNodeList* option_list = nullptr;

    if (ast_option_list != nullptr) {
        option_list = node_manager->MakeNodeList();
        for (const auto entry : ast_option_list->options_entries()) {
            node::SqlNode* node = nullptr;
            CHECK_STATUS(ConvertTableOption(entry, node_manager, &node))
            if (node != nullptr) {
                // NOTE: unhandled option will return OK, but node is not set
                option_list->PushBack(node);
            }
        }
    }

    auto* create = node_manager->MakeCreateTableNode(if_not_exist, db_name, table_name, column_desc_list, option_list);
    create->like_clause_.swap(like_clause);
    *output = create;

    return base::Status::OK();
}

base::Status ConvertCreateFunctionNode(const zetasql::ASTCreateFunctionStatement* ast_create_fun_stmt,
                                       node::NodeManager* node_manager, node::CreateFunctionNode** output) {
    node::TypeNode* tp;
    CHECK_STATUS(ConvertASTType(ast_create_fun_stmt->return_type(), node_manager, &tp));
    CHECK_TRUE(tp->IsBaseType(), common::kSqlAstError, "Un-support: func return type for non-basic type")
    auto function_declaration = ast_create_fun_stmt->function_declaration();
    CHECK_TRUE(function_declaration != nullptr, common::kSqlAstError, "not has function_declaration");
    std::string function_name;
    CHECK_STATUS(AstPathExpressionToString(function_declaration->name(), &function_name));
    std::vector<node::DataType> args;
    for (const auto param : function_declaration->parameters()->parameter_entries()) {
        node::TypeNode* ele_tp;
        CHECK_STATUS(ConvertASTType(param->type(), node_manager, &ele_tp));
        args.emplace_back(ele_tp->base());
    }
    auto options = std::make_shared<node::OptionsMap>();
    if (ast_create_fun_stmt->options_list() != nullptr) {
        CHECK_STATUS(ConvertAstOptionsListToMap(ast_create_fun_stmt->options_list(), node_manager, options));
    }
    // TOOD(ace): return & param type should better be the TypeNode
    *output = dynamic_cast<node::CreateFunctionNode*>(node_manager->MakeCreateFunctionNode(
        function_name, tp->base(), args, ast_create_fun_stmt->is_aggregate(), options));
    return base::Status::OK();
}

// ASTCreateProcedureStatement(name, parameters, body)
//   -> CreateSpStmt(name, parameters, body)
base::Status ConvertCreateProcedureNode(const zetasql::ASTCreateProcedureStatement* ast_create_sp_stmt,
                                        node::NodeManager* node_manager, node::CreateSpStmt** output) {
    std::string sp_name;
    CHECK_STATUS(AstPathExpressionToString(ast_create_sp_stmt->name(), &sp_name));

    node::SqlNodeList* procedure_parameters = node_manager->MakeNodeList();
    for (const auto param : ast_create_sp_stmt->parameters()->parameter_entries()) {
        node::SqlNode* param_node = nullptr;
        CHECK_STATUS(ConvertParamter(param, node_manager, &param_node));
        procedure_parameters->PushBack(param_node);
    }

    node::SqlNodeList* body = nullptr;
    CHECK_STATUS(ConvertProcedureBody(ast_create_sp_stmt->body(), node_manager, &body));

    *output =
        static_cast<node::CreateSpStmt*>(node_manager->MakeCreateProcedureNode(sp_name, procedure_parameters, body));
    return base::Status::OK();
}

// case element
//   ASTColumnDefinition -> case element.schema
//         ASSTSimpleColumnSchema -> ColumnDeefNode
//         otherwise              -> not implemented
//   ASTIndexDefinition  -> ColumnIndexNode
//   otherwise           -> not implemented
base::Status ConvertTableElement(const zetasql::ASTTableElement* element, node::NodeManager* node_manager,
                                 node::SqlNode** node) {
    base::Status status;

    switch (element->node_kind()) {
        case zetasql::AST_COLUMN_DEFINITION: {
            auto column_def = element->GetAsOrNull<zetasql::ASTColumnDefinition>();
            CHECK_TRUE(column_def != nullptr, common::kSqlAstError, "not an ASTColumnDefinition");

            auto not_null_columns = column_def->schema()->FindAttributes<zetasql::ASTNotNullColumnAttribute>(
                zetasql::AST_NOT_NULL_COLUMN_ATTRIBUTE);
            bool not_null = !not_null_columns.empty();

            const std::string name = column_def->name()->GetAsString();

            auto kind = column_def->schema()->node_kind();
            switch (kind) {
                case zetasql::AST_SIMPLE_COLUMN_SCHEMA: {
                    // only simple column schema is supported
                    auto simple_column_schema = column_def->schema()->GetAsOrNull<zetasql::ASTSimpleColumnSchema>();
                    CHECK_TRUE(simple_column_schema != nullptr, common::kSqlAstError, "not and ASTSimpleColumnSchema");

                    std::string type_name = "";
                    CHECK_STATUS(AstPathExpressionToString(simple_column_schema->type_name(), &type_name))
                    node::DataType type;
                    CHECK_STATUS(node::StringToDataType(type_name, &type));

                    node::ExprNode* default_value = nullptr;
                    if (simple_column_schema->default_expression()) {
                        CHECK_STATUS(
                            ConvertExprNode(simple_column_schema->default_expression(), node_manager, &default_value));
                    }

                    *node = node_manager->MakeColumnDescNode(name, type, not_null, default_value);
                    return base::Status::OK();
                }
                default: {
                    return base::Status(common::kSqlAstError, absl::StrCat("unsupported column schema type: ",
                                                                           zetasql::ASTNode::NodeKindToString(kind)));
                }
            }
            break;
        }
        case zetasql::AST_INDEX_DEFINITION: {
            auto ast_index_node = element->GetAsOrNull<zetasql::ASTIndexDefinition>();
            node::ColumnIndexNode* index_node = nullptr;
            CHECK_STATUS(ConvertColumnIndexNode(ast_index_node, node_manager, &index_node));
            *node = index_node;
            return base::Status::OK();
        }
        default: {
            return base::Status(common::kSqlAstError,
                                absl::StrCat("unsupported table column elemnt: ", element->GetNodeKindString()));
        }
    }
}

// ASTIndexDefinition node
//   map ConvertIndexOption node.option_list
base::Status ConvertColumnIndexNode(const zetasql::ASTIndexDefinition* ast_def_node, node::NodeManager* node_manager,
                                    node::ColumnIndexNode** output) {
    node::SqlNodeList* index_node_list = node_manager->MakeNodeList();
    for (const auto option : ast_def_node->options_list()->options_entries()) {
        node::SqlNode* node = nullptr;
        CHECK_STATUS(ConvertIndexOption(option, node_manager, &node));
        if (node != nullptr) {
            // NOTE: unhandled option will return OK, but node is not set
            index_node_list->PushBack(node);
        }
    }
    *output = static_cast<node::ColumnIndexNode*>(node_manager->MakeColumnIndexNode(index_node_list));
    return base::Status::OK();
}

// case entry->name()
//   "key"      -> IndexKeyNode
//   "ts"       -> IndexTsNode
//   "ttl"      -> IndexTTLNode
//   "ttl_type" -> IndexTTLTypeNode
//   "version"  -> IndexVersionNode
base::Status ConvertIndexOption(const zetasql::ASTOptionsEntry* entry, node::NodeManager* node_manager,
                                node::SqlNode** output) {
    auto name = entry->name()->GetAsString();
    absl::string_view name_v(name);
    if (absl::EqualsIgnoreCase("key", name_v)) {
        switch (entry->value()->node_kind()) {
            case zetasql::AST_PATH_EXPRESSION: {
                std::string column_name;
                CHECK_STATUS(
                    AstPathExpressionToString(entry->value()->GetAsOrNull<zetasql::ASTPathExpression>(), &column_name));
                *output = node_manager->MakeIndexKeyNode(column_name);

                return base::Status::OK();
            }
            case zetasql::AST_STRUCT_CONSTRUCTOR_WITH_PARENS: {
                auto ast_struct_expr = entry->value()->GetAsOrNull<zetasql::ASTStructConstructorWithParens>();
                CHECK_TRUE(ast_struct_expr != nullptr, common::kSqlAstError, "not a ASTStructConstructorWithParens");

                CHECK_TRUE(!ast_struct_expr->field_expressions().empty(), common::kSqlAstError,
                           "index key list is empty");

                int field_expr_len = ast_struct_expr->field_expressions().size();
                std::string key_str;
                CHECK_TRUE(zetasql::AST_PATH_EXPRESSION == ast_struct_expr->field_expression(0)->node_kind(),
                           common::kSqlAstError, "Invaid index key, should be path expression");
                CHECK_STATUS(AstPathExpressionToString(
                    ast_struct_expr->field_expression(0)->GetAsOrNull<zetasql::ASTPathExpression>(), &key_str));

                node::IndexKeyNode* index_keys =
                    dynamic_cast<node::IndexKeyNode*>(node_manager->MakeIndexKeyNode(key_str));

                for (int i = 1; i < field_expr_len; ++i) {
                    std::string key;
                    CHECK_TRUE(zetasql::AST_PATH_EXPRESSION == ast_struct_expr->field_expression(i)->node_kind(),
                               common::kSqlAstError, "Invaid index key, should be path expression");
                    CHECK_STATUS(AstPathExpressionToString(
                        ast_struct_expr->field_expression(i)->GetAsOrNull<zetasql::ASTPathExpression>(), &key));
                    index_keys->AddKey(key);
                }
                *output = index_keys;

                return base::Status::OK();
            }
            default: {
                return base::Status(common::kSqlAstError, absl::StrCat("unsupported key option value, type: ",
                                                                       entry->value()->GetNodeKindString()));
            }
        }
    } else if (absl::EqualsIgnoreCase("ts", name_v)) {
        std::string column_name;
        CHECK_TRUE(zetasql::AST_PATH_EXPRESSION == entry->value()->node_kind(), common::kSqlAstError,
                   "Invaid index ts, should be path expression");
        CHECK_STATUS(
            AstPathExpressionToString(entry->value()->GetAsOrNull<zetasql::ASTPathExpression>(), &column_name));
        *output = node_manager->MakeIndexTsNode(column_name);
        return base::Status::OK();
    } else if (absl::EqualsIgnoreCase("ttl", name_v)) {
        // case entry->value()
        //   ASTIntervalLiteral                  -> [ConstNode(kDay | kHour | kMinute)]
        //   ASTIntLiteral                       -> [ConstNode(kLatest)]
        //   (ASTIntervalLiteral, ASTIntLiteral) -> [ConstNode(kDay | kHour | kMinute), ConstNode)]
        auto ttl_list = node_manager->MakeExprList();
        switch (entry->value()->node_kind()) {
            case zetasql::AST_INTERVAL_LITERAL: {
                int64_t value;
                node::DataType unit;
                CHECK_STATUS(ASTIntervalLIteralToNum(entry->value(), &value, &unit));
                auto node = node_manager->MakeConstNode(value, unit);
                ttl_list->PushBack(node);
                break;
            }
            case zetasql::AST_INT_LITERAL: {
                int64_t value;
                CHECK_STATUS(ASTIntLiteralToNum(entry->value(), &value));
                auto node = node_manager->MakeConstNode(value, node::kLatest);
                ttl_list->PushBack(node);
                break;
            }
            case zetasql::AST_STRUCT_CONSTRUCTOR_WITH_PARENS: {
                const auto struct_parens = entry->value()->GetAsOrNull<zetasql::ASTStructConstructorWithParens>();
                CHECK_TRUE(struct_parens != nullptr, common::kSqlAstError, "not an ASTStructConstructorWithParens");
                CHECK_TRUE(struct_parens->field_expressions().size() == 2, common::kSqlAstError,
                           "ASTStructConstructorWithParens size != 2");

                int64_t value = 0;
                node::DataType unit;
                CHECK_STATUS(ASTIntervalLIteralToNum(struct_parens->field_expression(0), &value, &unit));

                auto node = node_manager->MakeConstNode(value, unit);
                ttl_list->PushBack(node);

                value = 0;
                CHECK_STATUS(ASTIntLiteralToNum(struct_parens->field_expression(1), &value));
                ttl_list->PushBack(node_manager->MakeConstNode(value, node::kLatest));
                break;
            }
            default: {
                FAIL_STATUS(common::kSqlAstError,
                            "unsupported ast expression type: ", entry->value()->GetNodeKindString());
            }
        }

        *output = node_manager->MakeIndexTTLNode(ttl_list);
        return base::Status::OK();
    } else if (absl::EqualsIgnoreCase("ttl_type", name_v)) {
        std::string ttl_type;
        CHECK_TRUE(zetasql::AST_PATH_EXPRESSION == entry->value()->node_kind(), common::kSqlAstError,
                   "Invalid ttl_type, should be path expression");
        CHECK_STATUS(AstPathExpressionToString(entry->value()->GetAsOrNull<zetasql::ASTPathExpression>(), &ttl_type));
        *output = node_manager->MakeIndexTTLTypeNode(ttl_type);
        return base::Status::OK();
    } else if (absl::EqualsIgnoreCase("version", name_v)) {
        switch (entry->value()->node_kind()) {
            case zetasql::AST_PATH_EXPRESSION: {
                std::string version;
                CHECK_TRUE(zetasql::AST_PATH_EXPRESSION == entry->value()->node_kind(), common::kSqlAstError,
                           "Invalid index version, should be path expression");
                CHECK_STATUS(
                    AstPathExpressionToString(entry->value()->GetAsOrNull<zetasql::ASTPathExpression>(), &version));
                *output = node_manager->MakeIndexVersionNode(version);
                return base::Status::OK();
            }
            case zetasql::AST_STRUCT_CONSTRUCTOR_WITH_PARENS: {
                // value is ( column_name, int_literal ), int_literal can be int or long number
                std::string column_name;
                const auto parens_struct = entry->value()->GetAsOrNull<zetasql::ASTStructConstructorWithParens>();
                CHECK_TRUE(parens_struct != nullptr, common::kSqlAstError, "not an ASTStructConstructorWithParens");
                CHECK_TRUE(
                    parens_struct->field_expressions().size() == 2, common::kSqlAstError,
                    "ASTStructConstructorWithParens has expression size = ", parens_struct->field_expressions().size());
                CHECK_TRUE(zetasql::AST_PATH_EXPRESSION == parens_struct->field_expression(0)->node_kind(),
                           common::kSqlAstError, "Invaid index version, should be path expression");
                CHECK_STATUS(AstPathExpressionToString(
                    parens_struct->field_expression(0)->GetAsOrNull<zetasql::ASTPathExpression>(), &column_name));
                int64_t val;
                CHECK_STATUS(ASTIntLiteralToNum(parens_struct->field_expression(1), &val))

                *output = node_manager->MakeIndexVersionNode(column_name, static_cast<int>(val));
                return base::Status::OK();
            }
            default: {
                return base::Status(common::kSqlAstError, absl::StrCat("unsupported node kind for index version: ",
                                                                       entry->value()->GetNodeKindString()));
            }
        }
    }

    return base::Status(common::kOk, absl::StrCat("index option ignored: ", name));
}

// case entry
//   ("partitionnum", int) -> PartitionNumNode(int)
//   ("replicanum", int)   -> ReplicaNumNode(int)
//   ("distribution", [ (string, [string] ) ] ) ->
base::Status ConvertTableOption(const zetasql::ASTOptionsEntry* entry, node::NodeManager* node_manager,
                                node::SqlNode** output) {
    auto identifier = entry->name()->GetAsString();
    absl::string_view identifier_v(identifier);
    if (absl::EqualsIgnoreCase("partitionnum", identifier_v)) {
        int64_t value = 0;
        CHECK_STATUS(ASTIntLiteralToNum(entry->value(), &value));
        *output = node_manager->MakePartitionNumNode(value);
    } else if (absl::EqualsIgnoreCase("replicanum", identifier_v)) {
        int64_t value = 0;
        CHECK_STATUS(ASTIntLiteralToNum(entry->value(), &value));
        *output = node_manager->MakeReplicaNumNode(value);
    } else if (absl::EqualsIgnoreCase("distribution", identifier_v)) {
        const auto arry_expr = entry->value()->GetAsOrNull<zetasql::ASTArrayConstructor>();
        CHECK_TRUE(arry_expr != nullptr, common::kSqlAstError, "distribution not and ASTArrayConstructor");
        CHECK_TRUE(!arry_expr->elements().empty(), common::kSqlAstError, "Un-support empty distributions currently")
        node::NodePointVector distribution_list;
        for (const auto e : arry_expr->elements()) {
            const auto ele = e->GetAsOrNull<zetasql::ASTStructConstructorWithParens>();
            if (ele == nullptr) {
                const auto arg = e->GetAsOrNull<zetasql::ASTStringLiteral>();
                CHECK_TRUE(arg != nullptr, common::kSqlAstError, "parse distribution failed");
                node::SqlNodeList* partition_mata_nodes = node_manager->MakeNodeList();
                partition_mata_nodes->PushBack(node_manager->MakePartitionMetaNode(node::RoleType::kLeader,
                            arg->string_value()));
                distribution_list.push_back(partition_mata_nodes);
            } else {
                node::SqlNodeList* partition_mata_nodes = node_manager->MakeNodeList();
                std::string leader;
                CHECK_STATUS(AstStringLiteralToString(ele->field_expression(0), &leader));
                partition_mata_nodes->PushBack(node_manager->MakePartitionMetaNode(node::RoleType::kLeader, leader));
                if (ele->field_expressions().size() > 1) {
                    const auto follower_list = ele->field_expression(1)->GetAsOrNull<zetasql::ASTArrayConstructor>();
                    CHECK_TRUE(follower_list != nullptr, common::kSqlAstError,
                            "follower element is not ASTArrayConstructor");
                    for (const auto fo_node : follower_list->elements()) {
                        std::string follower;
                        CHECK_STATUS(AstStringLiteralToString(fo_node, &follower));
                        partition_mata_nodes->PushBack(
                            node_manager->MakePartitionMetaNode(node::RoleType::kFollower, follower));
                    }
                }
                distribution_list.push_back(partition_mata_nodes);
            }
        }
        *output = node_manager->MakeDistributionsNode(distribution_list);
    } else if (absl::EqualsIgnoreCase("storage_mode", identifier_v)) {
        std::string storage_mode;
        CHECK_STATUS(AstStringLiteralToString(entry->value(), &storage_mode));
        absl::AsciiStrToLower(&storage_mode);
        *output = node_manager->MakeNode<node::StorageModeNode>(node::NameToStorageMode(storage_mode));
    } else if (absl::EqualsIgnoreCase("compress_type", identifier_v)) {
        std::string compress_type;
        CHECK_STATUS(AstStringLiteralToString(entry->value(), &compress_type));
        absl::AsciiStrToLower(&compress_type);
        auto ret = node::NameToCompressType(compress_type);
        if (ret.ok()) {
            *output = node_manager->MakeNode<node::CompressTypeNode>(*ret);
        } else {
            return base::Status(common::kSqlAstError, ret.status().ToString());
        }
    } else {
        return base::Status(common::kSqlAstError, absl::StrCat("invalid option ", identifier));
    }

    return base::Status::OK();
}

base::Status ConvertParamter(const zetasql::ASTFunctionParameter* param, node::NodeManager* node_manager,
                             node::SqlNode** output) {
    bool is_constant = param->is_constant();
    std::string column_name = param->name()->GetAsString();

    node::TypeNode* tp = nullptr;

    CHECK_TRUE(nullptr == param->templated_parameter_type() && nullptr == param->tvf_schema(), common::kSqlAstError,
               "Un-support templated_parameter or tvf_schema type")
    CHECK_TRUE(nullptr == param->alias(), common::kSqlAstError, "Un-support alias for parameter");

    // NOTE: only handle <type_> in ASTFunctionParameter here.
    //   consider handle <templated_parameter_type_>, <tvf_schema_>, <alias_> in the future,
    //   <templated_parameter_type_> and <tvf_schema_> is another syntax for procedure parameter,
    //   <alias_> is the additional syntax for function parameter
    CHECK_STATUS(ConvertASTType(param->type(), node_manager, &tp))
    CHECK_TRUE(tp->IsBaseType(), common::kSqlAstError, "Un-support: func parameter accept only basic type, but get ",
               tp->DebugString())
    *output = node_manager->MakeInputParameterNode(is_constant, column_name, tp->base());
    return base::Status::OK();
}

// ASTScript([
//   ASTBeginEndBlock(
//     [query:ASTQueryStatement])
//   ]) ->
// SqlNodeList([query])
base::Status ConvertProcedureBody(const zetasql::ASTScript* body, node::NodeManager* node_manager,
                                  node::SqlNodeList** output) {
    // HACK: for Procedure Body, there is only one statement which is BeginEndBlock
    CHECK_TRUE(
        body->statement_list().size() == 1 && zetasql::AST_BEGIN_END_BLOCK == body->statement_list()[0]->node_kind(),
        common::kSqlAstError, "procedure body must have one BeginEndBlock");
    node::SqlNode* body_node = nullptr;
    CHECK_STATUS(ConvertStatement(body->statement_list()[0], node_manager, &body_node));
    CHECK_TRUE(body_node->GetType() == node::kNodeList, common::kSqlAstError,
               "Inner error: procedure body is not converted to SqlNodeList");
    *output = dynamic_cast<node::SqlNodeList*>(body_node);
    return base::Status::OK();
}

base::Status ConvertASTScript(const zetasql::ASTScript* script, node::NodeManager* node_manager,
                              node::SqlNodeList** output) {
    CHECK_TRUE(nullptr != script, common::kSqlAstError, "Fail to convert ASTScript, script is null")
    *output = node_manager->MakeNodeList();
    for (auto statement : script->statement_list()) {
        CHECK_TRUE(nullptr != statement, common::kSqlAstError, "SQL Statement is null")
        node::SqlNode* stmt;
        CHECK_STATUS(ConvertStatement(statement, node_manager, &stmt));
        (*output)->PushBack(stmt);
    }
    return base::Status::OK();
}
// transform zetasql::ASTStringLiteral into string
base::Status AstStringLiteralToString(const zetasql::ASTExpression* ast_expr, std::string* str) {
    auto string_literal = ast_expr->GetAsOrNull<zetasql::ASTStringLiteral>();
    CHECK_TRUE(string_literal != nullptr, common::kSqlAstError, "not an ASTStringLiteral");

    *str = string_literal->string_value();
    return base::Status::OK();
}

// transform zetasql::ASTPathExpression into single string
// fail if path with multiple paths
base::Status AstPathExpressionToString(const zetasql::ASTPathExpression* path_expr, std::string* str) {
    CHECK_TRUE(path_expr != nullptr, common::kSqlAstError, "not an ASTPathExpression");
    CHECK_TRUE(path_expr->num_names() <= 1, common::kSqlAstError, "fail to convert multiple paths into a single string")
    *str = path_expr->num_names() == 0 ? "" : path_expr->name(0)->GetAsString();
    return base::Status::OK();
}
// transform zetasql::ASTPathExpression into string
base::Status AstPathExpressionToStringList(const zetasql::ASTPathExpression* path_expr,
                                           std::vector<std::string>& strs) {  // NOLINT
    CHECK_TRUE(path_expr != nullptr, common::kSqlAstError, "not an ASTPathExpression");
    for (int i = 0; i < path_expr->num_names(); i++) {
        CHECK_TRUE(nullptr != path_expr->name(i), common::kSqlAstError,
                   "fail to convert path expression to string: name[", i, "] is nullptr")
        strs.push_back(path_expr->name(i)->GetAsString());
    }
    return base::Status::OK();
}

// {integer literal} -> number
// {long literal}(l|L) -> number
// {hex literal} -> number
base::Status ASTIntLiteralToNum(const zetasql::ASTExpression* ast_expr, int64_t* val) {
    const auto int_literal = ast_expr->GetAsOrNull<zetasql::ASTIntLiteral>();
    CHECK_TRUE(int_literal != nullptr, common::kSqlAstError, "not an ASTIntLiteral");
    auto img = int_literal->image();
    if (int_literal->is_long()) {
        img.remove_suffix(1);
    }

    auto [status, ret] = udf::v1::StrToIntegral()(img);
    if (status.ok()) {
        *val = ret;
        return base::Status::OK();
    }
    FAIL_STATUS(common::kSqlAstError, "Invalid integer literal<", status.ToString(), ">");
}

// transform zetasql::ASTIntervalLiteral into (number, unit)
base::Status ASTIntervalLIteralToNum(const zetasql::ASTExpression* ast_expr, int64_t* val, node::DataType* unit) {
    const auto interval_literal = ast_expr->GetAsOrNull<zetasql::ASTIntervalLiteral>();
    CHECK_TRUE(interval_literal != nullptr, common::kSqlAstError, "not an ASTIntervalLiteral");

    switch (interval_literal->image().back()) {
        case 'd':
        case 'D':
            *unit = node::kDay;
            break;
        case 'h':
        case 'H':
            *unit = node::kHour;
            break;
        case 'm':
        case 'M':
            *unit = node::kMinute;
            break;
        case 's':
        case 'S':
            *unit = node::DataType::kSecond;
            break;
        default:
            FAIL_STATUS(common::kTypeError, "Invalid interval literal ", interval_literal->image(),
                        ": invalid interval unit")
    }
    auto img = interval_literal->image();
    img.remove_suffix(1);

    auto [status, ret] = udf::v1::StrToIntegral()(img);
    if (status.ok()) {
        *val = ret;
        return base::Status::OK();
    }
    FAIL_STATUS(common::kSqlAstError, "Invalid interval literal<", status.ToString(), ">");
}

base::Status ConvertDeleteNode(const zetasql::ASTDeleteStatement* delete_stmt, node::NodeManager* node_manager,
                                    node::DeleteNode** output) {
    auto id = delete_stmt->GetTargetPathForNonNested().value_or(nullptr);
    CHECK_TRUE(id != nullptr, common::kSqlAstError,
               "unsupported delete statement's target is not path expression");
    CHECK_TRUE(id->num_names() == 1 || id->num_names() == 2, common::kSqlAstError,
               "unsupported delete statement's target path has size > 2");
    auto id_name = id->first_name()->GetAsStringView();
    if (delete_stmt->where() != nullptr) {
        CHECK_TRUE(delete_stmt->GetTargetPathForNonNested().ok(), common::kSqlAstError,
                   "Un-support delete statement with illegal target table path")
        std::vector<std::string> names;
        CHECK_STATUS(AstPathExpressionToStringList(delete_stmt->GetTargetPathForNonNested().value(), names));
        CHECK_TRUE(!names.empty() && names.size() <= 2, common::kSqlAstError, "illegal name in delete sql");
        std::string db_name;
        std::string table_name = names.back();
        if (names.size() == 2) {
            db_name = names[0];
        }
        node::ExprNode* where_expr = nullptr;
        CHECK_STATUS(ConvertExprNode(delete_stmt->where(), node_manager, &where_expr));
        *output = node_manager->MakeDeleteNode(node::DeleteTarget::TABLE, "", db_name, table_name, where_expr);
    } else if (absl::EqualsIgnoreCase(id_name, "job")) {
        std::vector<absl::string_view> targets;
        CHECK_STATUS(ConvertTargetName(delete_stmt->opt_target_name(), targets));
        CHECK_TRUE(targets.size() == 1, common::kSqlAstError, "unsupported delete sql");
        *output = node_manager->MakeDeleteNode(node::DeleteTarget::JOB, targets.front(), "", "", nullptr);
    } else {
        FAIL_STATUS(common::kSqlAstError, "unsupported delete sql");
    }
    return base::Status::OK();
}

base::Status ConvertInsertStatement(const zetasql::ASTInsertStatement* root, node::NodeManager* node_manager,
                                    node::InsertStmt** output) {
    base::Status status;
    if (nullptr == root) {
        *output = nullptr;
        return base::Status::OK();
    }
    CHECK_TRUE(nullptr == root->query(), common::kSqlAstError, "Un-support insert statement with query");

    CHECK_TRUE(zetasql::ASTInsertStatement::InsertMode::DEFAULT_MODE == root->insert_mode(), common::kSqlAstError,
               "Un-support insert mode ", root->GetSQLForInsertMode());
    CHECK_TRUE(nullptr == root->returning(), common::kSqlAstError,
               "Un-support insert statement with return clause currently", root->GetSQLForInsertMode());
    CHECK_TRUE(nullptr == root->assert_rows_modified(), common::kSqlAstError,
               "Un-support insert statement with assert_rows_modified currently", root->GetSQLForInsertMode());

    node::ExprListNode* column_list = node_manager->MakeExprList();
    if (nullptr != root->column_list()) {
        for (auto column : root->column_list()->identifiers()) {
            column_list->PushBack(node_manager->MakeColumnRefNode(column->GetAsString(), ""));
        }
    }

    CHECK_TRUE(root->GetTargetPathForNonNested().ok(), common::kSqlAstError,
               "Un-support insert statement with illegal target table path")
    CHECK_TRUE(nullptr != root->rows(), common::kSqlAstError, "Un-support insert statement with empty values")
    node::ExprListNode* rows = node_manager->MakeExprList();
    for (auto row : root->rows()->rows()) {
        CHECK_TRUE(nullptr != row, common::kSqlAstError, "Un-support insert statement with null row")
        node::ExprListNode* row_values;
        CHECK_STATUS(ConvertExprNodeList(row->values(), node_manager, &row_values))
        for (auto expr : row_values->children_) {
            CHECK_TRUE(nullptr != expr &&
                           (node::kExprPrimary == expr->GetExprType() || node::kExprParameter == expr->GetExprType()),
                       common::kSqlAstError, "Un-support insert statement with un-const value")
        }
        rows->AddChild(row_values);
    }

    std::string table_name = "";
    std::string db_name = "";
    std::vector<std::string> names;
    CHECK_STATUS(AstPathExpressionToStringList(root->GetTargetPathForNonNested().value(), names));
    table_name = names.back();
    if (names.size() == 2) {
        db_name = names[0];
    }
    *output =
        dynamic_cast<node::InsertStmt*>(node_manager->MakeInsertTableNode(db_name, table_name, column_list, rows));
    return base::Status::OK();
}
base::Status ConvertDropStatement(const zetasql::ASTDropStatement* root, node::NodeManager* node_manager,
                                  node::CmdNode** output) {
    base::Status status;
    if (nullptr == root) {
        *output = nullptr;
        return base::Status::OK();
    }
    std::vector<std::string> names;
    CHECK_STATUS(AstPathExpressionToStringList(root->name(), names))
    switch (root->schema_object_kind()) {
        case zetasql::SchemaObjectKind::kTable: {
            CHECK_TRUE(2 >= names.size(), common::kSqlAstError, "Invalid table path expression ",
                       root->name()->ToIdentifierPathString())
            if (names.size() == 1) {
                *output =
                    dynamic_cast<node::CmdNode*>(node_manager->MakeCmdNode(node::CmdType::kCmdDropTable, names.back()));

            } else {
                *output = dynamic_cast<node::CmdNode*>(
                    node_manager->MakeCmdNode(node::CmdType::kCmdDropTable, names[0], names[1]));
            }
            (*output)->SetIfExists(root->is_if_exists());
            return base::Status::OK();
        }
        case zetasql::SchemaObjectKind::kDatabase: {
            CHECK_TRUE(1 == names.size(), common::kSqlAstError, "Invalid database path expression ",
                       root->name()->ToIdentifierPathString())
            *output =
                dynamic_cast<node::CmdNode*>(node_manager->MakeCmdNode(node::CmdType::kCmdDropDatabase, names[0]));
            (*output)->SetIfExists(root->is_if_exists());
            return base::Status::OK();
        }
        case zetasql::SchemaObjectKind::kIndex: {
            CHECK_TRUE(3 >= names.size() && names.size() >= 2, common::kSqlAstError, "Invalid index path expression ",
                       root->name()->ToIdentifierPathString())
            *output = dynamic_cast<node::CmdNode*>(
                node_manager->MakeCmdNode(node::CmdType::kCmdDropIndex, names));

            return base::Status::OK();
        }
        case zetasql::SchemaObjectKind::kProcedure: {
            CHECK_TRUE(2 >= names.size(), common::kSqlAstError, "Invalid table path expression ",
                       root->name()->ToIdentifierPathString())
            *output = dynamic_cast<node::CmdNode*>(node_manager->MakeCmdNode(node::CmdType::kCmdDropSp, names.back()));
            return base::Status::OK();
        }
        case zetasql::SchemaObjectKind::kDeployment: {
            CHECK_TRUE(2 >= names.size(), common::kSqlAstError, "Invalid deployment path expression ",
                       root->name()->ToIdentifierPathString())
            if (names.size() == 1) {
                *output =
                    dynamic_cast<node::CmdNode*>(node_manager->MakeCmdNode(node::CmdType::kCmdDropDeployment,
                                                                           names.back()));

            } else {
                *output =
                    dynamic_cast<node::CmdNode*>(node_manager->MakeCmdNode(node::CmdType::kCmdDropDeployment,
                                                                           names[0], names[1]));
            }
            return base::Status::OK();
        }
        default: {
            FAIL_STATUS(common::kSqlAstError, "Un-support DROP ", root->GetNodeKindString());
        }
    }
    return base::Status::OK();
}
base::Status ConvertCreateIndexStatement(const zetasql::ASTCreateIndexStatement* root, node::NodeManager* node_manager,
                                         node::CreateIndexNode** output) {
    CHECK_TRUE(nullptr != root, common::kSqlAstError, "not an ASTCreateIndexStatement")
    std::string index_name = "";
    CHECK_TRUE(nullptr != root->name(), common::kSqlAstError, "can't create index without index name");
    CHECK_STATUS(AstPathExpressionToString(root->name(), &index_name));

    std::vector<std::string> table_path;
    CHECK_TRUE(nullptr != root->table_name(), common::kSqlAstError, "can't create index without table");
    CHECK_STATUS(AstPathExpressionToStringList(root->table_name(), table_path));
    CHECK_TRUE(table_path.size() >= 1 && table_path.size() <= 2, common::kSqlAstError,
               "can't crete index with invalid table path")
    std::string db_name = "";
    std::string table_name = "";
    table_name = table_path.back();
    if (table_path.size() == 2) {
        db_name = table_path[0];
    }

    CHECK_TRUE(nullptr != root->index_item_list(), common::kSqlAstError, "can't create index with empty index items");

    std::vector<std::string> keys;
    for (const auto ordering_expression : root->index_item_list()->ordering_expressions()) {
        CHECK_TRUE(zetasql::AST_PATH_EXPRESSION == ordering_expression->expression()->node_kind(), common::kSqlAstError,
                   "Un-support index key type ", ordering_expression->expression()->GetNodeKindString())
        CHECK_TRUE(!ordering_expression->descending(), common::kSqlAstError, "Un-support descending index key")
        std::vector<std::string> path;
        CHECK_STATUS(AstPathExpressionToStringList(
            ordering_expression->expression()->GetAsOrNull<zetasql::ASTPathExpression>(), path));
        CHECK_TRUE(path.size() == 1, common::kSqlAstError, "Un-support index key path size = ", path.size());
        keys.push_back(path.back());
    }
    node::SqlNodeList* index_node_list = node_manager->MakeNodeList();

    node::SqlNode* index_key_node = node_manager->MakeIndexKeyNode(keys);
    index_node_list->PushBack(index_key_node);
    if (root->options_list() != nullptr) {
        for (const auto option : root->options_list()->options_entries()) {
            node::SqlNode* node = nullptr;
            CHECK_STATUS(ConvertIndexOption(option, node_manager, &node));
            if (node != nullptr) {
                // NOTE: unhandled option will return OK, but node is not set
                index_node_list->PushBack(node);
            }
        }
    }
    node::ColumnIndexNode* column_index_node =
        static_cast<node::ColumnIndexNode*>(node_manager->MakeColumnIndexNode(index_node_list));
    *output = dynamic_cast<node::CreateIndexNode*>(
        node_manager->MakeCreateIndexNode(index_name, db_name, table_name, column_index_node));
    return base::Status::OK();
}

base::Status ConvertAstOptionsListToMap(const zetasql::ASTOptionsList* options, node::NodeManager* node_manager,
                                        std::shared_ptr<node::OptionsMap> options_map, bool to_lower) {
    for (auto entry : options->options_entries()) {
        std::string key = entry->name()->GetAsString();
        if (to_lower) {
            boost::to_lower(key);
        }
        auto entry_value = entry->value();
        node::ExprNode* value = nullptr;
        CHECK_STATUS(ConvertExprNode(entry_value, node_manager, &value));
        CHECK_TRUE(value->GetExprType() == node::kExprPrimary, common::kSqlAstError,
                   "Unsupported value other than const type: ", entry_value->DebugString());
        options_map->emplace(key, dynamic_cast<const node::ConstNode*>(value));
    }
    return base::Status::OK();
}

base::Status ConvertTargetName(const zetasql::ASTTargetName* node, std::vector<absl::string_view>& output) { // NOLINT
    CHECK_TRUE(node != nullptr && node->target() != nullptr, common::kSqlAstError, "not an ASTTargetName");
    auto names = node->target();
    if (names->node_kind() == zetasql::AST_PATH_EXPRESSION) {
        const auto path_names = names->GetAsOrNull<const zetasql::ASTPathExpression>();
        CHECK_TRUE(path_names != nullptr, common::kSqlAstError, "not an ASTPathExpression");
        if (path_names->num_names() == 1) {
            output.push_back(path_names->first_name()->GetAsStringView());
        } else if (path_names->num_names() == 2) {
            output.push_back(path_names->first_name()->GetAsStringView());
            output.push_back(path_names->last_name()->GetAsStringView());
        } else {
            FAIL_STATUS(common::kSqlAstError, "Invalid target name: ", path_names->ToIdentifierPathString());
        }
    } else if (names->node_kind() == zetasql::AST_INT_LITERAL) {
        const auto int_name = names->GetAsOrNull<zetasql::ASTIntLiteral>();
        CHECK_TRUE(int_name != nullptr, common::kSqlAstError, "not an ASTIntLiteral");
        output.push_back(int_name->image());
    }
    return base::Status::OK();
}

struct ShowTargetInfo {
    node::CmdType cmd_type_;  // converted CmdType

    // whether show statement require extra target name must followed
    bool with_target_name_ = false;

    // whether show stmt allow optional like clause following
    bool with_like_string_ = false;
};

static const absl::flat_hash_map<std::string_view, ShowTargetInfo> showTargetMap = {
    {"DATABASES", {node::CmdType::kCmdShowDatabases}},
    {"TABLES", {node::CmdType::kCmdShowTables}},
    {"PROCEDURES", {node::CmdType::kCmdShowProcedures}},
    {"PROCEDURE STATUS", {node::CmdType::kCmdShowProcedures}},
    {"DEPLOYMENTS", {node::CmdType::kCmdShowDeployments}},
    {"JOBS", {node::CmdType::kCmdShowJobs}},
    {"VARIABLES", {node::CmdType::kCmdShowSessionVariables}},
    {"SESSION VARIABLES", {node::CmdType::kCmdShowSessionVariables}},
    {"GLOBAL VARIABLES", {node::CmdType::kCmdShowGlobalVariables}},
    {"CREATE PROCEDURE", {node::CmdType::kCmdShowCreateSp, true}},
    {"CREATE TABLE", {node::CmdType::kCmdShowCreateTable, true}},
    {"DEPLOYMENT", {node::CmdType::kCmdShowDeployment, true}},
    {"JOB", {node::CmdType::kCmdShowJob, true}},
    {"COMPONENTS", {node::CmdType::kCmdShowComponents}},
    {"TABLE STATUS", {node::CmdType::kCmdShowTableStatus, false, true}},
    {"FUNCTIONS", {node::CmdType::kCmdShowFunctions}},
    {"JOBLOG", {node::CmdType::kCmdShowJobLog, true}},
};

static const absl::flat_hash_map<std::string_view, node::ShowStmtType> SHOW_STMT_TYPE_MAP = {
    {"JOBS", node::ShowStmtType::kJobs},
};

base::Status convertShowStmt(const zetasql::ASTShowStatement* show_statement, node::NodeManager* node_manager,
                             node::SqlNode** output) {
    CHECK_TRUE(nullptr != show_statement && nullptr != show_statement->identifier(), common::kSqlAstError,
               "not an ASTShowStatement")

    auto show_id = show_statement->identifier()->GetAsStringView();
    // TODO(dl239): move all show statement from CmdNode to ShowNode
    if (auto iter = SHOW_STMT_TYPE_MAP.find(absl::AsciiStrToUpper(show_id));
            iter != SHOW_STMT_TYPE_MAP.end() && show_statement->optional_name() != nullptr) {
        std::vector<std::string> names;
        CHECK_STATUS(AstPathExpressionToStringList(show_statement->optional_name(), names));
        CHECK_TRUE(names.size() == 1, common::kSqlAstError, "illegal optional name in show statement");
        std::string like;
        if (show_statement->optional_like_string() != nullptr) {
            like = show_statement->optional_like_string()->string_value();
        }
        *output = node_manager->MakeNode<node::ShowNode>(iter->second, names.back(), like);
        return base::Status::OK();
    }

    auto show_info = showTargetMap.find(absl::AsciiStrToUpper(show_id));
    if (show_info == showTargetMap.end()) {
        FAIL_STATUS(common::kSqlAstError, "Un-support SHOW: ", show_id)
    }

    CHECK_TRUE(show_info->second.with_like_string_ || nullptr == show_statement->optional_like_string(),
               common::kSqlAstError, absl::StrCat("Non-support LIKE in SHOW ", show_info->first, " statement"))

    auto cmd_type = show_info->second.cmd_type_;
    if (show_info->second.with_target_name_) {
        CHECK_TRUE(show_statement->optional_target_name(), common::kSqlAstError,
                   absl::AsciiStrToUpper(node::CmdTypeName(show_info->second.cmd_type_)),
                   " statement require a target name following");

        std::vector<absl::string_view> path_list;
        CHECK_STATUS(ConvertTargetName(show_statement->optional_target_name(), path_list));
        if (path_list.size() == 1) {
            *output =
                node_manager->MakeCmdNode(cmd_type, std::string(path_list.front().data(), path_list.front().size()));
        } else if (path_list.size() == 2) {
            *output =
                node_manager->MakeCmdNode(cmd_type, std::string(path_list.front().data(), path_list.front().size()),
                                          std::string(path_list.back().data(), path_list.back().size()));
        }
        return base::Status::OK();
    }

    if (show_info->second.with_like_string_ && show_statement->optional_like_string()) {
        *output = node_manager->MakeCmdNode(cmd_type, show_statement->optional_like_string()->string_value());
        return base::Status::OK();
    }

    *output = dynamic_cast<node::CmdNode*>(node_manager->MakeCmdNode(cmd_type));
    return base::Status::OK();
}

base::Status ConvertArrayExpr(const zetasql::ASTArrayConstructor* array_expr, node::NodeManager* nm,
                              node::ExprNode** output) {
    auto* array = nm->MakeArrayExpr();
    for (auto expression : array_expr->elements()) {
        node::ExprNode* expr = nullptr;
        CHECK_STATUS(ConvertExprNode(expression, nm, &expr))
        array->AddChild(expr);
    }
    if (array_expr->type() != nullptr) {
        node::TypeNode* tp = nullptr;
        CHECK_STATUS(ConvertASTType(array_expr->type(), nm, &tp));
        // tp is TypeNode(kArray), not ArrayType, making new
        array->specific_type_ = nm->MakeArrayType(tp->GetGenericType(0), array->GetChildNum());
    }
    *output = array;
    return base::Status::OK();
}

base::Status ConvertASTType(const zetasql::ASTType* ast_type, node::NodeManager* nm, node::TypeNode** output) {
    CHECK_TRUE(nullptr != ast_type, common::kSqlAstError, "Un-support null ast type");
    CHECK_TRUE(ast_type->IsType(), common::kSqlAstError, "Un-support ast node ", ast_type->DebugString());

    switch (ast_type->node_kind()) {
        case zetasql::AST_SIMPLE_TYPE: {
            CHECK_STATUS((ConvertGuard<zetasql::ASTSimpleType, node::TypeNode>(
                ast_type, nm, output,
                [](const zetasql::ASTSimpleType* ast_simple, node::NodeManager* nm,
                   node::TypeNode** out) -> base::Status {
                    std::string type_name;
                    CHECK_STATUS(AstPathExpressionToString(ast_simple->type_name(), &type_name));

                    node::DataType type = node::kNull;
                    CHECK_STATUS(node::StringToDataType(type_name, &type))
                    *out = nm->MakeTypeNode(type);
                    return base::Status::OK();
                })));
            break;
        }
        case zetasql::AST_ARRAY_TYPE: {
            CHECK_STATUS((ConvertGuard<zetasql::ASTArrayType, node::TypeNode>(
                ast_type, nm, output,
                [](const zetasql::ASTArrayType* array_tp, node::NodeManager* nm, node::TypeNode** out) -> base::Status {
                    auto* tp = nm->MakeTypeNode(node::kArray);
                    node::TypeNode* elem_tp = nullptr;
                    CHECK_STATUS(ConvertASTType(array_tp->element_type(), nm, &elem_tp));
                    CHECK_TRUE(elem_tp->IsBaseType(), common::kTypeError,
                               "array of non-basic type is not supported: ARRAY<", elem_tp->DebugString(), ">");
                    // array permits NULL elements
                    tp->AddGeneric(elem_tp, true);
                    *out = tp;
                    return base::Status::OK();
                })));
            break;
        }
        default: {
            return base::Status(common::kSqlAstError, "Un-support type: " + ast_type->GetNodeKindString());
        }
    }
    return base::Status::OK();
}

base::Status ConvertWithClause(const zetasql::ASTWithClause* with_clause, node::NodeManager* nm,
                               base::BaseList<node::WithClauseEntry>* out) {
    for (auto clause : with_clause->with()) {
        node::QueryNode* query = nullptr;
        CHECK_STATUS(ConvertQueryNode(clause->query(), nm, &query));

        auto* with_entry = nm->MakeNode<node::WithClauseEntry>(clause->alias()->GetAsString(), query);

        out->data_.push_back(with_entry);
    }
    return base::Status::OK();
}

base::Status convertAlterAction(const zetasql::ASTAlterAction* action, node::NodeManager* nm,
                                node::AlterActionBase** out) {
    switch (action->node_kind()) {
        case zetasql::AST_ADD_PATH_ACTION: {
            node::AddPathAction* ac = nullptr;
            CHECK_STATUS(ConvertGuard<zetasql::ASTAddOfflinePathAction>(
                action, nm, &ac,
                [](const zetasql::ASTAddOfflinePathAction* in, node::NodeManager* nm, node::AddPathAction** out) {
                    *out = nm->MakeObj<node::AddPathAction>(in->path()->string_value());
                    return base::Status::OK();
                }));
            *out = ac;
            break;
        }
        case zetasql::AST_DROP_PATH_ACTION: {
            node::DropPathAction* ac = nullptr;
            CHECK_STATUS(ConvertGuard<zetasql::ASTDropOfflinePathAction>(
                action, nm, &ac,
                [](const zetasql::ASTDropOfflinePathAction* in, node::NodeManager* nm, node::DropPathAction** out) {
                    *out = nm->MakeObj<node::DropPathAction>(in->path()->string_value());
                    return base::Status::OK();
                }));
            *out = ac;
            break;
        }
        default:
            FAIL_STATUS(common::kUnsupportSql, action->SingleNodeDebugString());
    }
    return base::Status::OK();
}

base::Status ConvertAlterTableStmt(const zetasql::ASTAlterTableStatement* ast_node_, node::NodeManager* nm,
                                   node::SqlNode** out) {
    std::vector<const node::AlterActionBase *> actions;
    for (auto &ac : ast_node_->action_list()->actions()) {
        node::AlterActionBase *ac_out = nullptr;
        CHECK_STATUS(convertAlterAction(ac, nm, &ac_out));
        actions.push_back(ac_out);
    }

    auto path = ast_node_->path();
    if (path->num_names() == 1) {
        *out = nm->MakeNode<node::AlterTableStmt>("", path->name(0)->GetAsStringView(), actions);
    } else if (path->num_names() == 2) {
        *out = nm->MakeNode<node::AlterTableStmt>(path->name(0)->GetAsStringView(), path->name(1)->GetAsStringView(),
                                                  actions);
    } else {
        FAIL_STATUS(common::kSyntaxError, "invalid table name: ", path->ToIdentifierPathString());
    }

    return base::Status::OK();
}

}  // namespace plan
}  // namespace hybridse
