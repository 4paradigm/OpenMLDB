/*
 * Copyright 2021 4paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "passes/physical/condition_optimized.h"

#include <string>
#include <utility>
#include <vector>

namespace hybridse {
namespace passes {

using hybridse::vm::PhysicalFilterNode;
using hybridse::vm::PhysicalJoinNode;
using hybridse::vm::PhysicalOpType;
using hybridse::vm::PhysicalRequestJoinNode;

bool ConditionOptimized::JoinConditionOptimized(PhysicalBinaryNode* in,
                                                Join* join) {
    if (2 != in->producers().size()) {
        LOG(WARNING)
            << "Fail to Join Condition Optimized: input produces size isn't 2";
        return false;
    }
    node::ExprListNode and_conditions;
    if (!TransfromAndConditionList(join->condition_.condition(),
                                   &and_conditions)) {
        return false;
    }

    node::ExprListNode new_and_conditions;
    std::vector<ExprPair> condition_eq_pair;
    if (!TransformJoinEqualExprPair(in->GetProducer(0)->schemas_ctx(),
                                    in->GetProducer(1)->schemas_ctx(),
                                    &and_conditions, &new_and_conditions,
                                    condition_eq_pair)) {
        return false;
    }
    node::ExprListNode* left_keys = node_manager_->MakeExprList();
    node::ExprListNode* right_keys = node_manager_->MakeExprList();
    for (auto pair : condition_eq_pair) {
        right_keys->AddChild(pair.right_expr_);
        left_keys->AddChild(pair.left_expr_);
    }
    node::ExprNode* filter_condition =
        node_manager_->MakeAndExpr(&new_and_conditions);
    join->left_key_.set_keys(left_keys);
    join->right_key_.set_keys(right_keys);
    join->condition_.set_condition(filter_condition);
    return true;
}

bool ConditionOptimized::FilterConditionOptimized(PhysicalOpNode* in,
                                                  Filter* filter) {
    node::ExprListNode and_conditions;
    if (!TransfromAndConditionList(filter->condition_.condition(),
                                   &and_conditions)) {
        return false;
    }

    node::ExprListNode new_and_conditions;
    std::vector<ExprPair> condition_eq_pair;
    if (!TransformConstEqualExprPair(&and_conditions, &new_and_conditions,
                                     condition_eq_pair)) {
        return false;
    }
    node::ExprListNode* left_keys = node_manager_->MakeExprList();
    node::ExprListNode* right_keys = node_manager_->MakeExprList();
    for (auto pair : condition_eq_pair) {
        right_keys->AddChild(pair.right_expr_);
        left_keys->AddChild(pair.left_expr_);
    }
    node::ExprNode* filter_condition =
        node_manager_->MakeAndExpr(&new_and_conditions);
    filter->left_key_.set_keys(left_keys);
    filter->right_key_.set_keys(right_keys);
    filter->condition_.set_condition(filter_condition);
    return true;
}

bool ConditionOptimized::Transform(PhysicalOpNode* in,
                                   PhysicalOpNode** output) {
    *output = in;
    switch (in->GetOpType()) {
        case PhysicalOpType::kPhysicalOpJoin: {
            PhysicalJoinNode* join_op = dynamic_cast<PhysicalJoinNode*>(in);
            return JoinConditionOptimized(join_op, &join_op->join_);
        }
        case PhysicalOpType::kPhysicalOpRequestJoin: {
            PhysicalRequestJoinNode* join_op =
                dynamic_cast<PhysicalRequestJoinNode*>(in);
            return JoinConditionOptimized(join_op, &join_op->join_);
        }
        case PhysicalOpType::kPhysicalOpFilter: {
            PhysicalFilterNode* filter_op =
                dynamic_cast<PhysicalFilterNode*>(in);
            return FilterConditionOptimized(filter_op, &filter_op->filter_);
        }
        case PhysicalOpType::kPhysicalOpRequestUnion: {
            vm::PhysicalRequestUnionNode* request_union = dynamic_cast<vm::PhysicalRequestUnionNode*>(in);
            for (auto& kv : request_union->window_unions_.window_unions_) {
                PhysicalOpNode* new_n = nullptr;
                // treat window unions as standalone query here, calls Apply explictly
                if (Apply(kv.first, &new_n) && new_n != nullptr) {
                    kv.first = new_n;
                }
            }
            return true;
        }
        default: {
            return false;
        }
    }
}

// Transform condition expression some sub conditions
// e.g.
// condition : sub_expr1 and sub_expr2 and sub expr3
// and_condition_list [sub_expr1, sub_expr2, sub_expr3]
bool ConditionOptimized::TransfromAndConditionList(
    const node::ExprNode* condition, node::ExprListNode* and_condition_list) {
    if (nullptr == condition) {
        DLOG(WARNING) << "Skip ConditionOptimized: conditions: null condition";
        return false;
    }

    switch (condition->expr_type_) {
        case node::kExprUnary: {
            const node::UnaryExpr* expr =
                dynamic_cast<const node::UnaryExpr*>(condition);
            switch (expr->GetOp()) {
                case node::kFnOpBracket: {
                    return TransfromAndConditionList(expr->children_[0],
                                                     and_condition_list);
                }
                default: {
                    and_condition_list->AddChild(
                        const_cast<node::ExprNode*>(condition));
                    return true;
                }
            }
        }
        case node::kExprBinary: {
            const node::BinaryExpr* expr =
                dynamic_cast<const node::BinaryExpr*>(condition);
            switch (expr->GetOp()) {
                case node::kFnOpAnd: {
                    for (auto child : expr->children_) {
                        TransfromAndConditionList(child, and_condition_list);
                    }
                    return true;
                }
                // TODO(chenjing): NOT(expr OR expr) ==> AND (NOT expr) AND (NOT
                // expr)
                default: {
                    and_condition_list->AddChild(
                        const_cast<node::ExprNode*>(condition));
                    return true;
                }
            }
        }
        default: {
            and_condition_list->AddChild(
                const_cast<node::ExprNode*>(condition));
            return true;
        }
    }
}

bool ConditionOptimized::MakeConstEqualExprPair(
    const std::pair<node::ExprNode*, node::ExprNode*> expr_pair,
    const SchemasContext* right_schemas_ctx, ExprPair* output) {
    bool is_first_const = node::ExprIsConst(expr_pair.first);
    bool is_second_const = node::ExprIsConst(expr_pair.second);
    if (is_first_const && is_second_const) {
        return false;
    } else if (is_first_const) {
        // resolved second expr
        if (CheckExprDependOnChildOnly(expr_pair.second, right_schemas_ctx)
                .isOK()) {
            *output = {expr_pair.first, expr_pair.second};
            return true;
        }
    } else if (is_second_const) {
        // resolved first expr
        if (CheckExprDependOnChildOnly(expr_pair.first, right_schemas_ctx)
                .isOK()) {
            *output = {expr_pair.second, expr_pair.first};
            return true;
        }
    }
    return false;
}

// Transform equal condition to expression pair
// e.g. t1.col1 = t2.col1 -> pair(t1.col1, t2.col1)
bool ConditionOptimized::ExtractEqualExprPair(
    node::ExprNode* condition,
    std::pair<node::ExprNode*, node::ExprNode*>* expr_pair) {
    if (nullptr == expr_pair || nullptr == condition) {
        return false;
    }
    switch (condition->expr_type_) {
        case node::kExprUnary: {
            const node::UnaryExpr* expr =
                dynamic_cast<const node::UnaryExpr*>(condition);
            switch (expr->GetOp()) {
                case node::kFnOpBracket: {
                    return ExtractEqualExprPair(expr->children_[0], expr_pair);
                }
                default: {
                    return false;
                }
            }
        }
        case node::kExprBinary: {
            const node::BinaryExpr* expr =
                dynamic_cast<const node::BinaryExpr*>(condition);
            switch (expr->GetOp()) {
                case node::kFnOpEq: {
                    expr_pair->first = expr->children_[0];
                    expr_pair->second = expr->children_[1];
                    return true;
                }
                default: {
                    return false;
                }
            }
        }
        default: {
            return false;
        }
    }
}
// Return CosntExpr Equal Expr Pair
// Const Expr should be first of pair
bool ConditionOptimized::TransformConstEqualExprPair(
    node::ExprListNode* and_conditions, node::ExprListNode* out_condition_list,
    std::vector<ExprPair>& condition_eq_pair) {  // NOLINT
    for (auto expr : and_conditions->children_) {
        std::pair<node::ExprNode*, node::ExprNode*> expr_pair;
        if (!ExtractEqualExprPair(expr, &expr_pair)) {
            out_condition_list->AddChild(expr);
            continue;
        }
        if (node::ExprIsConst(expr_pair.first)) {
            condition_eq_pair.push_back({expr_pair.first, expr_pair.second});
        } else if (node::ExprIsConst(expr_pair.second)) {
            condition_eq_pair.push_back({expr_pair.second, expr_pair.first});
        } else {
            out_condition_list->AddChild(expr);
        }
    }
    return !condition_eq_pair.empty();
}
// Return Equal Expression Pair
// Left Expr should belong to first schema
bool ConditionOptimized::TransformJoinEqualExprPair(
    const SchemasContext* left_schemas_ctx,
    const SchemasContext* right_schemas_ctx, node::ExprListNode* and_conditions,
    node::ExprListNode* out_condition_list,
    std::vector<ExprPair>& condition_eq_pair) {  // NOLINT
    for (auto expr : and_conditions->children_) {
        std::pair<node::ExprNode*, node::ExprNode*> expr_pair;
        if (!ExtractEqualExprPair(expr, &expr_pair)) {
            out_condition_list->AddChild(expr);
            continue;
        }
        ExprPair const_pair;
        if (MakeConstEqualExprPair(expr_pair, right_schemas_ctx, &const_pair)) {
            condition_eq_pair.push_back(const_pair);
        } else {
            if (CheckExprDependOnChildOnly(expr_pair.first, left_schemas_ctx)
                    .isOK() &&
                CheckExprDependOnChildOnly(expr_pair.second, right_schemas_ctx)
                    .isOK()) {
                condition_eq_pair.push_back(
                    {expr_pair.first, expr_pair.second});
            } else if (CheckExprDependOnChildOnly(expr_pair.second,
                                                  left_schemas_ctx)
                           .isOK() &&
                       CheckExprDependOnChildOnly(expr_pair.first,
                                                  right_schemas_ctx)
                           .isOK()) {
                condition_eq_pair.push_back(
                    {expr_pair.second, expr_pair.first});
            } else {
                out_condition_list->AddChild(expr);
            }
        }
    }
    return !condition_eq_pair.empty();
}
void ConditionOptimized::SkipConstExpression(node::ExprListNode input,
                                             node::ExprListNode* output) {
    if (node::ExprListNullOrEmpty(&input)) {
        return;
    }
}
}  // namespace passes
}  // namespace hybridse
