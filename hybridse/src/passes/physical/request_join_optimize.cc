/**
 * Copyright (c) 2023 OpenMLDB Authors
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

#include "passes/physical/request_join_optimize.h"

#include <vector>

#include "absl/cleanup/cleanup.h"
#include "absl/status/status.h"
#include "node/sql_node.h"
#include "vm/physical_op.h"

namespace hybridse {
namespace passes {

RequestJoinOptimize::RequestJoinOptimize(PhysicalPlanContext* plan_ctx) : TransformUpPysicalPass(plan_ctx) {}

// RequestJoin
//   Filter
//     Table
// ->
// RequestJoin(JoinCond+=Filter.cond)
//   Table
bool RequestJoinOptimize::Transform(PhysicalOpNode* in, PhysicalOpNode** output) {
    *output = in;
    switch (in->GetOpType()) {
        case vm::kPhysicalOpRequestJoin: {
            auto request_join = dynamic_cast<vm::PhysicalRequestJoinNode*>(in);

            RightParseInfo info;
            auto s = CollectKidsInfo(request_join->GetProducer(1), &info);
            status_.Update(s);
            if (!s.ok()) {
                return false;
            }

            if (filters_.empty()) {
                // no filter, no need to optimzie
                return false;
            }

            auto filter = filters_.top();
            filters_.pop();

            // assume filter node has condition and index key only
            auto join_cond_expr = request_join->join().condition().condition();
            auto index_expr = request_join->join().index_key().keys();
            // replace expr with new schema
            auto new_schema_ctx = request_join->GetProducer(1)->schemas_ctx();
            auto old_schema_ctx = filter->schemas_ctx();

            if (filter != nullptr && filter->filter().condition().condition() != nullptr) {
                auto filter_cond = filter->filter().condition().condition();

                node::ExprNode* new_filter_cond = nullptr;
                status_.Update(BuildNewExprList(filter_cond, old_schema_ctx, new_schema_ctx, &new_filter_cond));
                if (!status_.ok()) {
                    return false;
                }

                auto new_cond_expr = new_filter_cond;
                if (join_cond_expr != nullptr) {
                    new_cond_expr = node_manager_->MakeBinaryExprNode(
                        const_cast<node::ExprNode*>(join_cond_expr->DeepCopy(node_manager_)), new_filter_cond,
                        node::kFnOpAnd);
                }

                // update join condition
                const_cast<vm::ConditionFilter&>(request_join->join().condition()).set_condition(new_cond_expr);
            }

            if (filter != nullptr && filter->filter().index_key().keys() != nullptr) {
                auto index_key = filter->filter().index_key().keys();

                node::ExprNode* new_index_key = nullptr;
                status_.Update(BuildNewExprList(index_key, old_schema_ctx, new_schema_ctx, &new_index_key));
                if (!status_.ok()) {
                    return false;
                }

                node::ExprListNode* new_index_expr = dynamic_cast<node::ExprListNode*>(new_index_key);
                if (index_expr != nullptr) {
                    for (auto child : index_expr->children_) {
                        new_index_expr->AddChild(child);
                    }
                }

                // update index key
                const_cast<vm::Key&>(request_join->join().index_key()).set_keys(new_index_expr);
            }

            *output = request_join;

            return true;
        }
        case vm::kPhysicalOpFilter: {
            // only LASTJOIN(REQUEST, FILTER) is supported online, standalone FILTER is not
            // so curently it is safe to simply rm the FILTER node
            auto filter = dynamic_cast<vm::PhysicalFilterNode*>(in);
            filters_.push(filter);
            // rm the filter node
            *output = filter->GetProducer(0);
            return true;
        }

        default:
            break;
    }
    return false;
}

absl::Status RequestJoinOptimize::CollectKidsInfo(PhysicalOpNode* kid, RightParseInfo* info) {
    if (kid->GetProducerCnt() > 1) {
        return absl::UnimplementedError("kids number > 1");
    }

    switch (kid->GetOpType()) {
        case vm::kPhysicalOpRename:
        case vm::kPhysicalOpSimpleProject:
        case vm::kPhysicalOpDataProvider: {
            info->data = dynamic_cast<vm::PhysicalDataProviderNode*>(kid);
            break;
        }

        default: {
            return absl::UnimplementedError(kid->GetTreeString());
        }
    }

    for (auto k : kid->GetProducers()) {
        return CollectKidsInfo(k, info);
    }

    return absl::OkStatus();
}

absl::Status RequestJoinOptimize::BuildNewExprList(const node::ExprNode* origin, const vm::SchemasContext* origin_sc,
                                                   const vm::SchemasContext* rebase_sc, node::ExprNode** out) {
    std::vector<const node::ExprNode*> depend_columns;
    node::ColumnOfExpression(origin, &depend_columns);
    passes::ExprReplacer replacer;
    for (auto expr : depend_columns) {
        auto s = vm::BuildColumnReplacement(expr, origin_sc, rebase_sc, node_manager_, &replacer);
        if (!s.isOK()) {
            return absl::InternalError(s.str());
        }
    }

    node::ExprNode* new_expr = nullptr;
    auto s = replacer.Replace(const_cast<node::ExprNode*>(origin), &new_expr);
    if (!s.isOK()) {
        return absl::InternalError(s.str());
    }
    *out = new_expr;

    return absl::OkStatus();
}

absl::Status RequestJoinOptimize::Sucess() const {
    if (!filters_.empty()) {
        return absl::UnimplementedError("multiple filter in same path or standalone FILTER without LASTJOIN");
    }
    return status_;
}

}  // namespace passes
}  // namespace hybridse
