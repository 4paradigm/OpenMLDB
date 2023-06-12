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

#include "passes/physical/window_column_pruning.h"

#include <algorithm>
#include <set>
#include <string>
#include <vector>

namespace hybridse {
namespace passes {

using hybridse::common::kPlanError;
using hybridse::vm::ColumnProjects;
using hybridse::vm::kPhysicalOpProject;
using hybridse::vm::kWindowAggregation;
using hybridse::vm::PhysicalProjectNode;
using hybridse::vm::PhysicalSimpleProjectNode;

Status WindowColumnPruning::Apply(PhysicalPlanContext* ctx,
                                  PhysicalOpNode* input, PhysicalOpNode** out) {
    cache_.clear();
    return DoApply(ctx, input, out);
}

Status WindowColumnPruning::DoApply(PhysicalPlanContext* ctx,
                                    PhysicalOpNode* input,
                                    PhysicalOpNode** out) {
    CHECK_TRUE(input != nullptr, kPlanError);
    auto cache_iter = cache_.find(input->node_id());
    if (cache_iter != cache_.end()) {
        *out = cache_iter->second;
        return Status::OK();
    }
    bool changed = false;
    std::vector<PhysicalOpNode*> children;
    for (size_t i = 0; i < input->GetProducerCnt(); ++i) {
        auto origin_child = input->GetProducer(i);
        PhysicalOpNode* new_child = nullptr;
        CHECK_STATUS(DoApply(ctx, origin_child, &new_child));
        if (new_child != origin_child) {
            changed = true;
        }
        children.push_back(new_child);
    }
    if (changed) {
        PhysicalOpNode* new_input = nullptr;
        CHECK_STATUS(ctx->WithNewChildren(input, children, &new_input));
        input = new_input;
    }
    *out = input;
    switch (input->GetOpType()) {
        case kPhysicalOpProject: {
            auto project_op = dynamic_cast<PhysicalProjectNode*>(input);
            if (project_op->project_type_ != kWindowAggregation) {
                break;
            }
            auto agg_op =
                dynamic_cast<PhysicalWindowAggrerationNode*>(project_op);
            if (agg_op->window_joins_.Empty() && !agg_op->need_append_input()) {
                CHECK_STATUS(ProcessWindow(ctx, agg_op, out));
            }
            break;
        }
        default:
            break;
    }
    cache_[input->node_id()] = *out;
    return Status::OK();
}

struct ColIndexInfo {
    size_t cid;
    size_t schema_idx;
    size_t col_idx;
    ColIndexInfo(size_t cid, size_t schema_idx, size_t col_idx)
        : cid(cid), schema_idx(schema_idx), col_idx(col_idx) {}
};

Status WindowColumnPruning::ProcessWindow(PhysicalPlanContext* ctx,
                                          PhysicalWindowAggrerationNode* agg_op,
                                          PhysicalOpNode** out) {
    auto input = agg_op->GetProducer(0);
    auto input_schema = input->schemas_ctx();

    // window op depends
    std::vector<const node::ExprNode*> depend_columns;
    agg_op->window().ResolvedRelatedColumns(&depend_columns);

    // projection depends
    const auto& projects = agg_op->project();
    for (size_t i = 0; i < projects.size(); ++i) {
        std::vector<const node::ExprNode*> project_depend_columns;
        CHECK_STATUS(input_schema->ResolveExprDependentColumns(
            projects.GetExpr(i), &project_depend_columns));
        std::copy(project_depend_columns.begin(), project_depend_columns.end(),
                  std::back_inserter(depend_columns));
    }

    // collect all columns to depend on: (column id, slice index, column index)
    std::vector<ColIndexInfo> column_indexes;
    std::set<size_t> column_id_set;

    for (const auto col_expr : depend_columns) {
        size_t cid;
        size_t schema_idx;
        size_t col_idx;
        switch (col_expr->GetExprType()) {
            case node::kExprColumnRef: {
                auto col = dynamic_cast<const node::ColumnRefNode*>(col_expr);
                CHECK_STATUS(input_schema->ResolveColumnRefIndex(
                    col, &schema_idx, &col_idx));
                cid = input_schema->GetSchemaSource(schema_idx)
                          ->GetColumnID(col_idx);
                break;
            }
            case node::kExprColumnId: {
                auto col = dynamic_cast<const node::ColumnIdNode*>(col_expr);
                CHECK_STATUS(input_schema->ResolveColumnIndexByID(
                    col->GetColumnID(), &schema_idx, &col_idx));
                cid = col->GetColumnID();
                break;
            }
            default:
                break;
        }
        if (column_id_set.find(cid) != column_id_set.end()) {
            continue;
        }
        column_id_set.insert(cid);
        column_indexes.push_back(ColIndexInfo(cid, schema_idx, col_idx));
    }

    // sort by column index
    size_t col_num = input_schema->GetColumnNum();
    std::sort(column_indexes.begin(), column_indexes.end(),
              [col_num](const ColIndexInfo& l, const ColIndexInfo& r) {
                  return l.schema_idx * col_num + l.col_idx <
                         r.schema_idx * col_num + r.col_idx;
              });

    // create column pruning
    ColumnProjects pruned_projects;
    for (size_t i = 0; i < column_indexes.size(); ++i) {
        const auto& info = column_indexes[i];
        auto col_expr = ctx->node_manager()->MakeColumnIdNode(info.cid);
        std::string name = input_schema->GetSchemaSource(info.schema_idx)
                               ->GetColumnName(info.col_idx);
        pruned_projects.Add(name, col_expr, nullptr);
    }
    PhysicalSimpleProjectNode* pruned_project_op = nullptr;
    CHECK_STATUS(ctx->CreateOp<PhysicalSimpleProjectNode>(
        &pruned_project_op, input, pruned_projects));

    // create column pruning on window unions
    std::vector<PhysicalOpNode*> pruned_unions;
    for (const auto& pair : agg_op->window_unions().window_unions_) {
        auto schemas_ctx = pair.first->schemas_ctx();
        ColumnProjects union_pruned_projects;
        for (size_t i = 0; i < column_indexes.size(); ++i) {
            const auto& info = column_indexes[i];
            auto source = schemas_ctx->GetSchemaSource(info.schema_idx);
            auto col_expr = ctx->node_manager()->MakeColumnIdNode(
                source->GetColumnID(info.col_idx));
            std::string name = source->GetColumnName(info.col_idx);
            union_pruned_projects.Add(name, col_expr, nullptr);
        }
        PhysicalSimpleProjectNode* union_op = nullptr;
        CHECK_STATUS(ctx->CreateOp<PhysicalSimpleProjectNode>(
            &union_op, pair.first, union_pruned_projects));
        pruned_unions.push_back(union_op);
    }

    // create new agg op
    PhysicalWindowAggrerationNode* new_agg_op = nullptr;
    CHECK_STATUS(ctx->CreateOp<PhysicalWindowAggrerationNode>(
        &new_agg_op, pruned_project_op, projects, agg_op->window(),
        agg_op->instance_not_in_window(), agg_op->need_append_input(),
        agg_op->exclude_current_time()));
    for (auto union_op : pruned_unions) {
        new_agg_op->AddWindowUnion(union_op);
    }
    *out = new_agg_op;
    return Status::OK();
}

}  // namespace passes
}  // namespace hybridse
