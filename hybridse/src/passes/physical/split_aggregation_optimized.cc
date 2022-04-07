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
#include "passes/physical/split_aggregation_optimized.h"

#include <vector>

#include "vm/engine.h"
#include "vm/physical_op.h"

namespace hybridse {
namespace passes {

SplitAggregationOptimized::SplitAggregationOptimized(PhysicalPlanContext* plan_ctx) : TransformUpPysicalPass(plan_ctx) {
    std::vector<std::string> windows;
    const auto* options = plan_ctx_->GetOptions();
    if (!options) {
        LOG(ERROR) << "plan_ctx option is empty";
        return;
    }

    boost::split(windows, options->at(vm::LONG_WINDOWS), boost::is_any_of(","));
    for (auto& w : windows) {
        std::vector<std::string> window_info;
        boost::split(window_info, w, boost::is_any_of(":"));
        boost::trim(window_info[0]);
        long_windows_.insert(window_info[0]);
    }
}

bool SplitAggregationOptimized::Transform(PhysicalOpNode* in, PhysicalOpNode** output) {
    *output = in;
    if (vm::kPhysicalOpProject != in->GetOpType()) {
        return false;
    }

    if (long_windows_.empty()) {
        LOG(ERROR) << "Long Windows is empty";
        return false;
    }

    auto project_op = dynamic_cast<vm::PhysicalProjectNode*>(in);
    if (project_op->project_type_ != vm::kAggregation) {
        return false;
    }

    auto project_aggr_op = dynamic_cast<vm::PhysicalAggrerationNode*>(project_op);
    const auto& projects = project_aggr_op->project();
    for (size_t i = 0; i < projects.size(); i++) {
        const auto* expr = projects.GetExpr(i);
        if (expr->GetExprType() == node::kExprCall) {
            const auto* call_expr = dynamic_cast<const node::CallExprNode*>(expr);
            const auto* window = call_expr->GetOver();
            if (window == nullptr) continue;

            // skip ANONYMOUS_WINDOW
            if (!window->GetName().empty()) {
                if (long_windows_.count(window->GetName())) {
                    return SplitProjects(project_aggr_op, output);
                }
            }
        }
    }
    return false;
}

bool SplitAggregationOptimized::SplitProjects(vm::PhysicalAggrerationNode* in, PhysicalOpNode** output) {
    const auto& projects = in->project();
    *output = in;

    if (!IsSplitable(in)) {
        LOG(INFO) << in->GetTreeString() << " is not splitable";
        return false;
    }

    DLOG(INFO) << "Split expr: " << in->GetTreeString();

    std::vector<vm::PhysicalProjectNode*> split_nodes;
    vm::ColumnProjects final_column_projects;
    vm::ColumnProjects simple_column_projects;
    for (size_t i = 0; i < projects.size(); i++) {
        const auto* expr = projects.GetExpr(i);
        auto name = projects.GetName(i);
        final_column_projects.Add(name, node_manager_->MakeColumnRefNode(name, ""), nullptr);

        if (expr->GetExprType() == node::kExprCall) {
            const auto* call_expr = dynamic_cast<const node::CallExprNode*>(expr);
            const auto* window = call_expr->GetOver();

            if (window) {
                // create PhysicalAggrerationNode of the window project
                vm::ColumnProjects column_projects;
                column_projects.Add(name, expr, projects.GetFrame(i));
                column_projects.SetPrimaryFrame(projects.GetPrimaryFrame());
                vm::PhysicalAggrerationNode* node = nullptr;
                DLOG(INFO) << "Create Single Aggregation: size = " << column_projects.size()
                           << ", expr = " << column_projects.GetExpr(column_projects.size() - 1)->GetExprString();

                auto status = plan_ctx_->CreateOp<vm::PhysicalAggrerationNode>(
                    &node, in->GetProducer(0), column_projects, in->having_condition_.condition());
                if (!status.isOK()) {
                    LOG(ERROR) << "Fail to create PhysicalAggrerationNode: " << status;
                    return false;
                }

                split_nodes.emplace_back(node);
                column_projects.Clear();
            } else {
                simple_column_projects.Add(projects.GetName(i), expr, projects.GetFrame(i));
            }
        } else {
            simple_column_projects.Add(projects.GetName(i), expr, projects.GetFrame(i));
        }
    }

    if (simple_column_projects.size() > 0) {
        vm::PhysicalRowProjectNode* row_prj = nullptr;
        auto status =
            plan_ctx_->CreateOp<vm::PhysicalRowProjectNode>(&row_prj, in->GetProducer(0), simple_column_projects);
        if (!status.isOK()) {
            LOG(ERROR) << "Fail to create PhysicalRowProjectNode: " << status;
            return false;
        }
        split_nodes.emplace_back(row_prj);
    }

    if (split_nodes.size() < 2) {
        return false;
    }

    vm::PhysicalRequestJoinNode* join = nullptr;
    auto status = plan_ctx_->CreateOp<vm::PhysicalRequestJoinNode>(&join, split_nodes[0], split_nodes[1],
                                                                   ::hybridse::node::kJoinTypeConcat);
    if (!status.isOK()) {
        LOG(ERROR) << "Fail to create PhysicalRequestJoinNode: " << status;
        return false;
    }

    for (size_t i = 2; i < split_nodes.size(); ++i) {
        vm::PhysicalRequestJoinNode* new_join = nullptr;
        status = plan_ctx_->CreateOp<vm::PhysicalRequestJoinNode>(&new_join, join, split_nodes[i],
                                                                  ::hybridse::node::kJoinTypeConcat);
        if (!status.isOK()) {
            LOG(ERROR) << "Fail to create PhysicalRequestJoinNode: " << status;
            return false;
        }
        join = new_join;
    }

    vm::PhysicalSimpleProjectNode* simple_prj = nullptr;
    status = plan_ctx_->CreateOp<vm::PhysicalSimpleProjectNode>(&simple_prj, join, final_column_projects);
    if (!status.isOK()) {
        LOG(ERROR) << "Fail to create PhysicalSimpleProjectNode: " << status;
        return false;
    }

    *output = simple_prj;
    return true;
}

bool SplitAggregationOptimized::IsSplitable(vm::PhysicalAggrerationNode* op) {
    // TODO(zhanghao): currently we only split the aggregation project that depends on a single physical table
    if (op->project().size() <= 1 || op->producers()[0]->GetOpType() != vm::kPhysicalOpRequestUnion) {
        return false;
    }

    auto req_union_op = dynamic_cast<vm::PhysicalRequestUnionNode*>(op->producers()[0]);
    return req_union_op->window_unions_.Empty();
}

}  // namespace passes
}  // namespace hybridse
