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

#include "passes/physical/cluster_optimized.h"
#include <string>
#include <vector>

namespace hybridse {
namespace passes {

using hybridse::vm::ColumnProjects;
using hybridse::vm::Join;
using hybridse::vm::PhysicalJoinNode;
using hybridse::vm::PhysicalOpType;
using hybridse::vm::PhysicalRequestJoinNode;
using hybridse::vm::PhysicalSimpleProjectNode;
using hybridse::vm::SchemasContext;

bool ClusterOptimized::SimplifyJoinLeftInput(
    PhysicalOpNode* join_op, const Join& join,
    const SchemasContext* joined_schema_ctx, PhysicalOpNode** output) {
    auto left = join_op->GetProducer(0);
    std::vector<const hybridse::node::ExprNode*> columns;
    std::vector<std::string> column_names;
    join.ResolvedRelatedColumns(&columns);

    std::ostringstream oss;
    for (auto column : columns) {
        oss << node::ExprString(column) << ",";
    }
    LOG(INFO) << "join resolved related columns: " << oss.str();

    // find columns belong to left side
    std::vector<size_t> left_indices;
    for (size_t i = 0; i < columns.size(); ++i) {
        if (CheckExprDependOnChildOnly(columns[i], left->schemas_ctx())
                .isOK()) {
            left_indices.push_back(i);
        }
    }

    // replace with column id expression
    for (size_t i = 0; i < columns.size(); ++i) {
        auto column_expr = columns[i];
        column_names.push_back(column_expr->GetExprString());
        if (column_expr->GetExprType() == node::kExprColumnRef) {
            size_t column_id;
            auto column_ref =
                dynamic_cast<const node::ColumnRefNode*>(column_expr);
            Status status = joined_schema_ctx->ResolveColumnID(column_ref->GetDBName(),
                column_ref->GetRelationName(), column_ref->GetColumnName(),
                &column_id);
            if (!status.isOK()) {
                LOG(WARNING)
                    << "Resolve join column " << column_ref->GetExprString()
                    << " failed: " << status;
                return false;
            }
            columns[i] = node_manager_->MakeColumnIdNode(column_id);
        }
    }

    ColumnProjects simplified_projects;
    for (size_t idx : left_indices) {
        auto column_expr = columns[idx];
        simplified_projects.Add(column_names[idx], column_expr, nullptr);
    }
    if (simplified_projects.size() >= left->GetOutputSchemaSize()) {
        DLOG(WARNING) << "Simplify columns size == left output columns";
        return false;
    }
    auto root = left;
    while (root->GetProducerCnt() > 0) {
        bool found_child = false;
        for (size_t i = 0; i < root->GetProducerCnt(); ++i) {
            auto schemas_ctx = root->GetProducer(i)->schemas_ctx();
            bool flag = true;
            for (size_t idx : left_indices) {
                auto column_expr = columns[idx];
                if (!CheckExprDependOnChildOnly(column_expr, schemas_ctx)
                         .isOK()) {
                    flag = false;
                    break;
                }
            }
            if (flag) {
                found_child = true;
                root = root->GetProducer(i);
                break;
            }
        }
        if (!found_child) {
            break;
        }
    }
    // try to simplify depend node
    PhysicalSimpleProjectNode* root_simplify_project_op = nullptr;
    auto status = plan_ctx_->CreateOp<PhysicalSimpleProjectNode>(
        &root_simplify_project_op, root, simplified_projects);
    if (!status.isOK()) {
        LOG(WARNING)
            << "Simplify root left input failed: apply left node simplify: "
            << status;
        return false;
    }
    DLOG(INFO) << "apply root node simplify: " << root_simplify_project_op->GetTreeString();
    *output = root_simplify_project_op;
    return true;
}

bool ClusterOptimized::Transform(PhysicalOpNode* in, PhysicalOpNode** output) {
    if (nullptr == in) {
        DLOG(WARNING) << "cluster optimized skip: node is null";
        return false;
    }
    switch (in->GetOpType()) {
        case PhysicalOpType::kPhysicalOpRequestJoin: {
            auto join_op = dynamic_cast<PhysicalRequestJoinNode*>(in);
            switch (join_op->join().join_type()) {
                case node::kJoinTypeLeft:
                case node::kJoinTypeLast: {
                    auto left = join_op->producers()[0];
                    auto right = join_op->producers()[1];
                    if (vm::PhysicalSchemaType::kSchemaTypeRow ==
                        right->GetOutputType()) {
                        DLOG(INFO)
                            << "request join optimized skip: row and row join";
                        return false;
                    }
                    auto simplify_left = left;
                    if (!SimplifyJoinLeftInput(join_op, join_op->join(),
                                               join_op->joined_schemas_ctx(),
                                               &simplify_left)) {
                        DLOG(WARNING) << "Simplify join left input failed";
                    }
                    Status status;
                    PhysicalRequestJoinNode* request_join_right_only = nullptr;
                    status = plan_ctx_->CreateOp<PhysicalRequestJoinNode>(
                        &request_join_right_only, simplify_left, right,
                        join_op->join(), true);
                    if (!status.isOK()) {
                        return false;
                    }
                    status = ReplaceComponentExpr(
                        join_op->join(), join_op->joined_schemas_ctx(),
                        request_join_right_only->joined_schemas_ctx(),
                        plan_ctx_->node_manager(),
                        &request_join_right_only->join_);
                    if (!status.isOK()) {
                        return false;
                    }

                    PhysicalRequestJoinNode* concat_op = nullptr;
                    status = plan_ctx_->CreateOp<PhysicalRequestJoinNode>(
                        &concat_op, left, request_join_right_only,
                        hybridse::node::kJoinTypeConcat);
                    if (!status.isOK()) {
                        return false;
                    }
                    *output = concat_op;
                    return true;
                }
                default: {
                    break;
                }
            }
            break;
        }
        case PhysicalOpType::kPhysicalOpJoin: {
            auto join_op = dynamic_cast<PhysicalJoinNode*>(in);
            switch (join_op->join().join_type()) {
                case node::kJoinTypeLeft:
                case node::kJoinTypeLast: {
                    auto left = join_op->producers()[0];
                    auto right = join_op->producers()[1];
                    auto simplify_left = left;
                    if (!SimplifyJoinLeftInput(join_op, join_op->join(),
                                               join_op->joined_schemas_ctx(),
                                               &simplify_left)) {
                        DLOG(WARNING) << "Simplify join left input failed";
                    }
                    Status status;
                    PhysicalJoinNode* join_right_only = nullptr;
                    status = plan_ctx_->CreateOp<PhysicalJoinNode>(
                        &join_right_only, simplify_left, right, join_op->join(),
                        true);
                    if (!status.isOK()) {
                        return false;
                    }
                    status = ReplaceComponentExpr(
                        join_op->join(), join_op->joined_schemas_ctx(),
                        join_right_only->joined_schemas_ctx(),
                        plan_ctx_->node_manager(), &join_right_only->join_);
                    if (!status.isOK()) {
                        return false;
                    }

                    vm::PhysicalJoinNode* concat_op = nullptr;
                    status = plan_ctx_->CreateOp<vm::PhysicalJoinNode>(
                        &concat_op, left, join_right_only,
                        hybridse::node::kJoinTypeConcat);
                    if (!status.isOK()) {
                        return false;
                    }
                    *output = concat_op;
                    return true;
                }
                default: {
                    break;
                }
            }
            break;
        }
        default: {
            return false;
        }
    }
    return false;
}
}  // namespace passes
}  // namespace hybridse
