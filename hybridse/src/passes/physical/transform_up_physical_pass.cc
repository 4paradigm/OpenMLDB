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
#include "passes/physical/transform_up_physical_pass.h"

#include <set>

namespace hybridse {
namespace passes {


std::string PhysicalPlanPassTypeName(PhysicalPlanPassType type) {
    switch (type) {
        case kPassSimpleProjectsOptimized:
            return "PassSimpleProjectsOptimized";
        case kPassFilterOptimized:
            return "PassFilterOptimized";
        case kPassGroupAndSortOptimized:
            return "PassGroupByOptimized";
        case kPassLeftJoinOptimized:
            return "PassLeftJoinOptimized";
        case kPassLimitOptimized:
            return "PassLimitOptimized";
        case kPassClusterOptimized:
            return "PassClusterOptimized";
        case kPassLongWindowOptimized:
            return "PassLongWindowOptimized";
        case kPassSplitAggregationOptimized:
            return "SplitAggregationOptimized";
        case kPassRequestJoinOptimze:
            return "PassRequestJoinOptimize";
        default:
            return "unknowPass";
    }
}

/// Transform every node with a optimization strategy in a post-DFS order
/// the parent will update it's producer list and SchemasContext after one
/// transform success
bool TransformUpPysicalPass::Apply(PhysicalOpNode* in, PhysicalOpNode** out) {
    if (nullptr == in || nullptr == out) {
        LOG(WARNING) << "fail to apply pass: input or output is null";
        return false;
    }
    if (visited_ids_.find(in->GetNodeId()) != visited_ids_.end()) {
        DLOG(INFO) << "Apply " << in->GetTypeName() << " node #" << in->GetNodeId() << " has been visited already.";
        *out = in;
        return visited_ids_[in->GetNodeId()];
    }
    auto producer = in->producers();
    for (size_t j = 0; j < producer.size(); ++j) {
        PhysicalOpNode* output = nullptr;
        if (Apply(producer[j], &output)) {
            if (!ResetProducer(plan_ctx_, in, j, output)) {
                visited_ids_[in->GetNodeId()] = false;
                return false;
            }
        }
    }
    in->ClearSchema();
    Status status = in->InitSchema(plan_ctx_);
    if (!status.isOK()) {
        LOG(WARNING) << "Reset schema failed: " << status;
        visited_ids_[in->GetNodeId()] = false;
        return false;
    }
    in->FinishSchema();
    visited_ids_[in->GetNodeId()] = Transform(in, out);
    return visited_ids_[in->GetNodeId()];
}

bool ResetProducer(PhysicalPlanContext* plan_ctx, PhysicalOpNode* op,
                   size_t idx, PhysicalOpNode* child) {
    auto origin = op->GetProducer(idx);
    if (origin == child) {
        return true;
    }
    op->SetProducer(idx, child);
    op->ClearSchema();
    Status status = op->InitSchema(plan_ctx);
    if (!status.isOK()) {
        LOG(WARNING) << "Reset producer failed: " << status << "\nAt child:\n"
                     << *child;
        op->SetProducer(idx, origin);
        op->ClearSchema();
        status = op->InitSchema(plan_ctx);
        if (!status.isOK()) {
            LOG(WARNING) << "Recover schema failed: " << status;
        }
        op->FinishSchema();
        return false;
    }
    op->FinishSchema();
    return true;
}

Status CheckExprDependOnChildOnly(const node::ExprNode* expr,
                                  const vm::SchemasContext* child_schemas_ctx) {
    std::set<size_t> column_ids;
    return child_schemas_ctx->ResolveExprDependentColumns(expr, &column_ids);
}

}  // namespace passes
}  // namespace hybridse
