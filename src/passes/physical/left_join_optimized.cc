 * Copyright (c) 2021 4paradigm
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
#include "passes/physical/left_join_optimized.h"

#include <string>

namespace fesql {
namespace passes {

bool LeftJoinOptimized::Transform(PhysicalOpNode* in, PhysicalOpNode** output) {
    if (nullptr == in) {
        DLOG(WARNING) << "LeftJoin optimized skip: node is null";
        return false;
    }
    if (in->producers().empty() || nullptr == in->producers()[0] ||
        vm::kPhysicalOpJoin != in->producers()[0]->GetOpType()) {
        return false;
    }
    vm::PhysicalJoinNode* join_op =
        dynamic_cast<vm::PhysicalJoinNode*>(in->producers()[0]);

    auto join_type = join_op->join().join_type();
    if (node::kJoinTypeLeft != join_type && node::kJoinTypeLast != join_type) {
        // skip optimized for other join type
        return false;
    }
    switch (in->GetOpType()) {
        case vm::kPhysicalOpGroupBy: {
            auto group_op = dynamic_cast<vm::PhysicalGroupNode*>(in);
            if (node::ExprListNullOrEmpty(group_op->group_.keys_)) {
                DLOG(WARNING) << "Join optimized skip: groups is null or empty";
            }

            if (!CheckExprListFromSchema(
                    group_op->group_.keys_,
                    join_op->GetProducers()[0]->GetOutputSchema())) {
                return false;
            }
            auto group_expr = group_op->group_.keys_;
            // 符合优化条件
            vm::PhysicalGroupNode* new_group_op = nullptr;
            Status status = plan_ctx_->CreateOp<vm::PhysicalGroupNode>(
                &new_group_op, join_op->producers()[0], group_expr);
            if (!status.isOK()) {
                return false;
            }
            vm::PhysicalJoinNode* new_join_op = nullptr;
            status = plan_ctx_->CreateOp<vm::PhysicalJoinNode>(
                &new_join_op, new_group_op, join_op->GetProducers()[1],
                join_op->join_);
            if (!status.isOK()) {
                return false;
            }
            *output = new_join_op;
            return true;
        }
        case vm::kPhysicalOpSortBy: {
            auto sort_op = dynamic_cast<vm::PhysicalSortNode*>(in);
            if (nullptr == sort_op->sort_.orders_ ||
                node::ExprListNullOrEmpty(sort_op->sort_.orders_->order_by_)) {
                DLOG(WARNING) << "Join optimized skip: order is null or empty";
            }
            if (!CheckExprListFromSchema(
                    sort_op->sort_.orders_->order_by_,
                    join_op->GetProducers()[0]->GetOutputSchema())) {
                return false;
            }
            // 符合优化条件
            vm::PhysicalSortNode* new_order_op = nullptr;
            Status status = plan_ctx_->CreateOp<vm::PhysicalSortNode>(
                &new_order_op, join_op->producers()[0], sort_op->sort_);
            if (!status.isOK()) {
                return false;
            }
            vm::PhysicalJoinNode* new_join_op = nullptr;
            status = plan_ctx_->CreateOp<vm::PhysicalJoinNode>(
                &new_join_op, new_order_op, join_op->GetProducers()[1],
                join_op->join_);
            if (!status.isOK()) {
                return false;
            }
            *output = new_join_op;
            return true;
        }
        case vm::kPhysicalOpProject: {
            auto project_op = dynamic_cast<vm::PhysicalProjectNode*>(in);
            if (vm::kWindowAggregation != project_op->project_type_) {
                return false;
            }

            if (node::kJoinTypeLast != join_type) {
                DLOG(WARNING) << "Window Join optimized skip: join type should "
                                 "be LAST JOIN, but "
                              << node::JoinTypeName(join_type);
                return false;
            }
            auto window_agg_op =
                dynamic_cast<vm::PhysicalWindowAggrerationNode*>(in);
            if (node::ExprListNullOrEmpty(
                    window_agg_op->window_.partition_.keys_) &&
                (nullptr == window_agg_op->window_.sort_.orders_ ||
                 node::ExprListNullOrEmpty(
                     window_agg_op->window_.sort_.orders_->order_by_))) {
                DLOG(WARNING)
                    << "Window Join optimized skip: both partition and"
                       "order are empty ";
                return false;
            }
            auto left_schemas_ctx = join_op->GetProducer(0)->schemas_ctx();
            if (!CheckExprDependOnChildOnly(
                     window_agg_op->window_.partition_.keys_, left_schemas_ctx)
                     .isOK()) {
                DLOG(WARNING) << "Window Join optimized skip: partition keys "
                                 "are resolved from secondary table";
                return false;
            }
            if (!CheckExprDependOnChildOnly(
                     window_agg_op->window_.sort_.orders_->order_by_,
                     left_schemas_ctx)
                     .isOK()) {
                DLOG(WARNING) << "Window Join optimized skip: order keys are "
                                 "resolved from secondary table";
                return false;
            }

            auto left = join_op->producers()[0];
            auto right = join_op->producers()[1];
            window_agg_op->AddWindowJoin(right, join_op->join());
            if (!ResetProducer(plan_ctx_, window_agg_op, 0, left)) {
                return false;
            }
            Transform(window_agg_op, output);
            *output = window_agg_op;
            return true;
        }
        default: {
            return false;
        }
    }
}
bool LeftJoinOptimized::ColumnExist(const Schema& schema,
                                    const std::string& column_name) {
    for (int32_t i = 0; i < schema.size(); i++) {
        const type::ColumnDef& column = schema.Get(i);
        if (column_name == column.name()) {
            return true;
        }
    }
    return false;
}

bool LeftJoinOptimized::CheckExprListFromSchema(
    const node::ExprListNode* expr_list, const Schema* schema) {
    if (node::ExprListNullOrEmpty(expr_list)) {
        return true;
    }

    for (auto expr : expr_list->children_) {
        switch (expr->expr_type_) {
            case node::kExprColumnRef: {
                auto column = dynamic_cast<node::ColumnRefNode*>(expr);
                if (!ColumnExist(*schema, column->GetColumnName())) {
                    return false;
                }
                break;
            }
            default: {
                // can't optimize when group by other expression
                return false;
            }
        }
    }
    return true;
}
}  // namespace passes
}  // namespace fesql
