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
#include <string>
#include <vector>

namespace hybridse {
namespace plan {

base::Status ConvertExprNode(const zetasql::ASTExpression* ast_expression, node::NodeManager* node_manager,
                             node::ExprNode** output) {
    if (nullptr == ast_expression) {
        *output = nullptr;
        return base::Status::OK();
    }
    base::Status status;
    // TODO(chenjing): support case when value and case when without value
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
                *output = node_manager->MakeColumnRefNode(path_expression->name(0)->GetAsString(),
                                                          path_expression->name(1)->GetAsString());
            } else if (3 == num_names) {
                *output = node_manager->MakeColumnRefNode(path_expression->name(0)->GetAsString(),
                                                          path_expression->name(1)->GetAsString(),
                                                          path_expression->name(2)->GetAsString());
            } else {
                status.code = common::kSqlError;
                status.msg = "Invalid column path expression " + path_expression->ToIdentifierPathString();
                return status;
            }
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
                case zetasql::ASTBinaryExpression::Op::LIKE: {
                    op = node::FnOperator::kFnOpLike;
                    break;
                }
                default: {
                    status.msg = "Unsupport binary operator: " + binary_expression->GetSQLForOperator();
                    status.code = common::kSqlError;
                    return status;
                }
            }
            *output = node_manager->MakeBinaryExprNode(lhs, rhs, op);
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
                    break;
                }
                case zetasql::ASTUnaryExpression::Op::NOT: {
                    op = node::FnOperator::kFnOpNot;
                    break;
                }
                case zetasql::ASTUnaryExpression::Op::PLUS: {
                    op = node::FnOperator::kFnOpAdd;
                    break;
                }
                default: {
                    status.msg = "Unsupport unary operator: " + unary_expression->GetSQLForOperator();
                    status.code = common::kSqlError;
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
            CHECK_TRUE(nullptr != lhs, common::kSqlError, "Invalid AND expression")

            for (size_t i = 1; i < and_expression->conjuncts().size(); i++) {
                node::ExprNode* rhs = nullptr;
                CHECK_STATUS(ConvertExprNode(and_expression->conjuncts(i), node_manager, &rhs))
                CHECK_TRUE(nullptr != rhs, common::kSqlError, "Invalid AND expression")
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
            CHECK_TRUE(nullptr != lhs, common::kSqlError, "Invalid OR expression")
            for (size_t i = 1; i < or_expression->disjuncts().size(); i++) {
                node::ExprNode* rhs = nullptr;
                CHECK_STATUS(ConvertExprNode(or_expression->disjuncts()[i], node_manager, &rhs))
                CHECK_TRUE(nullptr != rhs, common::kSqlError, "Invalid OR expression")
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
            *output =  node_manager->MakeBetweenExpr(expr, low, high, between_expression->is_not());

            return base::Status();
        }
        case zetasql::AST_FUNCTION_CALL: {
            auto* function_call = ast_expression->GetAsOrDie<zetasql::ASTFunctionCall>();
            node::ExprListNode* args = nullptr;
            CHECK_TRUE(false == function_call->HasModifiers(), common::kSqlError,
                       "Un-support Modifiers for function call")
            CHECK_STATUS(ConvertExprNodeList(function_call->arguments(), node_manager, &args))
            *output = node_manager->MakeFuncNode(function_call->function()->ToIdentifierPathString(), args, nullptr);
            return base::Status::OK();
        }
        case zetasql::AST_ANALYTIC_FUNCTION_CALL: {
            auto* analytic_function_call = ast_expression->GetAsOrDie<zetasql::ASTAnalyticFunctionCall>();

            node::ExprNode* function_call = nullptr;
            node::WindowDefNode* over_winodw = nullptr;
            CHECK_STATUS(ConvertExprNode(analytic_function_call->function(), node_manager, &function_call));
            CHECK_STATUS(ConvertWindowSpecification(analytic_function_call->window_spec(), node_manager, &over_winodw))

            if (nullptr != function_call) {
                dynamic_cast<node::CallExprNode*>(function_call)->SetOver(over_winodw);
            }
            *output = function_call;
            return base::Status::OK();
        }
        case zetasql::AST_INT_LITERAL: {
            const zetasql::ASTIntLiteral* literal = ast_expression->GetAsOrDie<zetasql::ASTIntLiteral>();
            int64_t int_value;
            bool is_null;

            if (literal->is_hex()) {
                status.msg = "Un-support hex integer literal: " + std::string(literal->image());
                status.code = common::kSqlError;
                return status;
            } else if (literal->is_long()) {
                hybridse::codec::StringRef str(std::string(literal->image().substr(0, literal->image().size() - 1)));
                hybridse::udf::v1::string_to_bigint(&str, &int_value, &is_null);
                if (is_null) {
                    status.msg = "Invalid long integer literal: " + std::string(literal->image());
                    status.code = common::kSqlError;
                    return status;
                }
                *output = node_manager->MakeConstNode(int_value);
            } else {
                hybridse::codec::StringRef str(literal->image().data());
                hybridse::udf::v1::string_to_bigint(&str, &int_value, &is_null);
                if (is_null) {
                    status.msg = "Invalid integer literal: " + std::string(literal->image());
                    status.code = common::kSqlError;
                    return status;
                }
                if (int_value <= INT_MAX && int_value >= INT_MIN) {
                    *output = node_manager->MakeConstNode(static_cast<int>(int_value));
                } else {
                    *output = node_manager->MakeConstNode(int_value);
                }
            }
            return base::Status::OK();
        }

        case zetasql::AST_STRING_LITERAL: {
            const zetasql::ASTStringLiteral* literal = ast_expression->GetAsOrDie<zetasql::ASTStringLiteral>();
            *output = node_manager->MakeConstNode(literal->string_value());
            return base::Status::OK();
        }

        case zetasql::AST_BOOLEAN_LITERAL: {
            const zetasql::ASTBooleanLiteral* literal = ast_expression->GetAsOrDie<zetasql::ASTBooleanLiteral>();
            bool bool_value;
            hybridse::codec::StringRef str(literal->image().data());
            bool is_null;
            hybridse::udf::v1::string_to_bool(&str, &bool_value, &is_null);
            if (is_null) {
                status.msg = "Invalid bool literal: " + std::string(literal->image());
                status.code = common::kSqlError;
                return status;
            }
            *output = node_manager->MakeConstNode(bool_value);
            return base::Status::OK();
        }
        case zetasql::AST_FLOAT_LITERAL: {
            const zetasql::ASTFloatLiteral* literal = ast_expression->GetAsOrDie<zetasql::ASTFloatLiteral>();

            bool is_null;
            if (literal->is_float32()) {
                float float_value = 0.0;
                hybridse::codec::StringRef str(std::string(literal->image().substr(0, literal->image().size() - 1)));
                hybridse::udf::v1::string_to_float(&str, &float_value, &is_null);
                if (is_null) {
                    status.msg = "Invalid float literal: " + std::string(literal->image());
                    status.code = common::kSqlError;
                    return status;
                }
                *output = node_manager->MakeConstNode(float_value);
            } else {
                double double_value = 0.0;
                hybridse::codec::StringRef str(literal->image().data());
                hybridse::udf::v1::string_to_double(&str, &double_value, &is_null);
                if (is_null) {
                    status.msg = "Invalid double literal: " + std::string(literal->image());
                    status.code = common::kSqlError;
                    return status;
                }
                *output = node_manager->MakeConstNode(double_value);
            }
            return base::Status::OK();
        }
        case zetasql::AST_INTERVAL_LITERAL: {
            const zetasql::ASTIntervalLiteral* literal = ast_expression->GetAsOrDie<zetasql::ASTIntervalLiteral>();
            int64_t interval_value;
            node::DataType interval_unit = node::DataType::kSecond;
            size_t image_len = literal->image().size();
            hybridse::codec::StringRef str(std::string(literal->image().substr(0, image_len - 1)));
            switch (literal->image().data()[image_len - 1]) {
                case 'h':
                case 'H': {
                    interval_unit = node::DataType::kHour;
                    break;
                }
                case 's':
                case 'S': {
                    interval_unit = node::DataType::kSecond;
                    break;
                }
                case 'm':
                case 'M': {
                    interval_unit = node::DataType::kMinute;
                    break;
                }
                case 'd':
                case 'D': {
                    interval_unit = node::DataType::kDay;
                    break;
                }
                default: {
                    status.msg = "Invalid interval literal: " + std::string(literal->image());
                    status.code = common::kSqlError;
                    return status;
                }
            }
            bool is_null;
            hybridse::udf::v1::string_to_bigint(&str, &interval_value, &is_null);
            if (is_null) {
                status.msg = "Invalid interval literal: " + std::string(literal->image());
                status.code = common::kSqlError;
                return status;
            }
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
            status.msg = "Un-support literal expression for node kind " + ast_expression->GetNodeKindString();
            status.code = common::kSqlError;
            return status;
        }

        default: {
            status.msg = "Unsupport ASTExpression " + ast_expression->GetNodeKindString();
            status.code = common::kSqlError;
            return status;
        }
    }
    return status;
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
        ordering_expressions->AddChild(expr);
        is_asc_list.push_back(!ordering_expression->descending());
    }

    *output = node_manager->MakeOrderByNode(ordering_expressions, is_asc_list);
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
                status.code = common::kSqlError;
                status.msg = "Invalid column path expression " + path_expression->ToIdentifierPathString();
                return status;
            }
            break;
        }
        default: {
            status.code = common::kSqlError;
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
            status.code = common::kSqlError;
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
            status.code = common::kSqlError;
            return status;
        }
    }
    node::FrameBound* start = nullptr;
    node::FrameBound* end = nullptr;
    CHECK_STATUS(ConvertFrameBound(window_frame->start_expr(), node_manager, &start))
    CHECK_STATUS(ConvertFrameBound(window_frame->end_expr(), node_manager, &end))
    node::ExprNode* frame_size = nullptr;
    if (nullptr != window_frame->max_size()) {
        CHECK_STATUS(ConvertExprNode(window_frame->max_size()->max_size(), node_manager, &frame_size))
    }
    *output = dynamic_cast<node::FrameNode*>(
        node_manager->MakeFrameNode(frame_type, node_manager->MakeFrameExtent(start, end), frame_size));
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
        CHECK_STATUS(ConvertFrameNode(window_spec->window_frame(), node_manager, &frame_node))
    }
    // TODO(chenjing): fill the following flags
    bool instance_is_not_in_window = window_spec->is_instance_not_in_window();
    bool exclude_current_time = window_spec->is_exclude_current_time();
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
        union_tables, partition_by, order_by, frame_node, exclude_current_time, instance_is_not_in_window));
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

            CHECK_TRUE(nullptr == table_path_expression->pivot_clause(), common::kSqlError, "Un-support pivot clause")
            CHECK_TRUE(nullptr == table_path_expression->unpivot_clause(), common::kSqlError,
                       "Un-support unpivot clause")
            CHECK_TRUE(nullptr == table_path_expression->for_system_time(), common::kSqlError, "Un-support system time")
            CHECK_TRUE(nullptr == table_path_expression->with_offset(), common::kSqlError,
                       "Un-support scan WITH OFFSET")
            CHECK_TRUE(nullptr == table_path_expression->sample_clause(), common::kSqlError,
                       "Un-support tablesample clause")
            CHECK_TRUE(nullptr == table_path_expression->hint(), common::kSqlError, "Un-support hint")

            CHECK_TRUE(table_path_expression->path_expr()->num_names() <= 2, common::kSqlError,
                       "Invalid table path expression ", table_path_expression->path_expr()->ToIdentifierPathString());
            std::string alias_name =
                nullptr != table_path_expression->alias() ? table_path_expression->alias()->GetAsString() : "";
            *output =
                node_manager->MakeTableNode(table_path_expression->path_expr()->last_name()->GetAsString(), alias_name);
            break;
        }
        case zetasql::AST_JOIN: {
            auto join = root->GetAsOrDie<zetasql::ASTJoin>();
            CHECK_TRUE(nullptr == join->hint(), common::kSqlError, "Un-support hint with join")

            CHECK_TRUE(zetasql::ASTJoin::JoinHint::NO_JOIN_HINT == join->join_hint(), common::kSqlError,
                       "Un-support join hint with join ", join->GetSQLForJoinHint())
            CHECK_TRUE(nullptr == join->using_clause(), common::kSqlError, "Un-support USING clause with join ")
            CHECK_TRUE(false == join->natural(), common::kSqlError, "Un-support natural with join ")
            node::TableRefNode* left = nullptr;
            node::TableRefNode* right = nullptr;
            node::OrderByNode* order_by = nullptr;
            node::ExprNode* condition = nullptr;
            node::JoinType join_type = node::JoinType::kJoinTypeInner;
            CHECK_STATUS(ConvertTableExpressionNode(join->lhs(), node_manager, &left))
            CHECK_STATUS(ConvertTableExpressionNode(join->rhs(), node_manager, &right))
            CHECK_STATUS(ConvertOrderBy(join->order_by(), node_manager, &order_by))
            CHECK_STATUS(ConvertExprNode(join->on_clause()->expression(), node_manager, &condition))
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
                default: {
                    status.msg = "Un-support join type " + join->GetSQLForJoinType();
                    status.code = common::kSqlError;
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
            status.code = common::kPlanError;
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
                                    node::SqlNode ** output) {
    base::Status status;
    if (nullptr == limit_offset) {
        *output = nullptr;
        return base::Status::OK();
    }

    CHECK_TRUE(nullptr == limit_offset->offset(), common::kSqlError, "Un-support OFFSET")
    CHECK_TRUE(nullptr != limit_offset->limit(), common::kSqlError, "Un-support LIMIT with null expression")

    node::ExprNode* limit = nullptr;
    CHECK_STATUS(ConvertExprNode(limit_offset->limit(), node_manager, &limit))
    CHECK_TRUE(node::kExprPrimary == limit->GetExprType(), common::kSqlError, "Un-support LIMIT with expression type ",
               limit_offset->GetNodeKindString())
    node::ConstNode* value = dynamic_cast<node::ConstNode*>(limit);
    switch (value->GetDataType()) {
        case node::kInt16:
        case node::kInt32:
        case node::kInt64: {
            *output = node_manager->MakeLimitNode(value->GetAsInt64());
            return base::Status::OK();
        }
        default: {
            status.code = common::kSqlError;
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
                                                  group_expr_list, having_expr, order_by, window_list_ptr, limit);
            return base::Status::OK();
        }
        default: {
            status.msg =
                "can not create query plan node with invalid query type " + query_expression->GetNodeKindString();
            status.code = common::kPlanError;
            return status;
        }
    }
    return base::Status::OK();
}
}  // namespace plan
}  // namespace hybridse
