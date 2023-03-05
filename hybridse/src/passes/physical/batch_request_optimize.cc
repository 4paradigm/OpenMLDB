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

#include "passes/physical/batch_request_optimize.h"
#include <set>
#include <vector>
#include "vm/physical_op.h"

namespace hybridse {
namespace passes {

using hybridse::common::kPlanError;
using hybridse::vm::ColumnProjects;
using hybridse::vm::DataProviderType;
using hybridse::vm::PhysicalJoinNode;
using hybridse::vm::PhysicalOpType;
using hybridse::vm::PhysicalPostRequestUnionNode;
using hybridse::vm::PhysicalRequestProviderNodeWithCommonColumn;
using hybridse::vm::PhysicalRowProjectNode;
using hybridse::vm::PhysicalSchemaType;
using hybridse::vm::PhysicalTableProjectNode;
using hybridse::vm::ProjectType;
using hybridse::vm::Range;

CommonColumnOptimize::CommonColumnOptimize(
    const std::set<size_t> common_column_indices)
    : common_column_indices_(common_column_indices) {}

bool CommonColumnOptimize::FindRequestUnionPath(
    PhysicalOpNode* root, std::vector<PhysicalOpNode*>* path) {
    if (root->GetOutputType() == PhysicalSchemaType::kSchemaTypeRow) {
        return false;
    }
    path->push_back(root);
    if (root->GetOpType() == PhysicalOpType::kPhysicalOpRequestUnion) {
        return dynamic_cast<PhysicalRequestUnionNode*>(root)
            ->output_request_row();
    }
    if (root->producers().size() == 0) {
        return false;
    }
    return FindRequestUnionPath(root->GetProducer(0), path);
}

void CommonColumnOptimize::Init() {
    output_common_column_indices_.clear();
    build_dict_.clear();
}

static Status GetSingleSliceResult(PhysicalPlanContext* ctx,
                                   PhysicalOpNode* input,
                                   PhysicalOpNode** output) {
    if (input == nullptr) {
        *output = nullptr;
        return Status::OK();
    }
    if (input->GetOutputSchemaSourceSize() == 1) {
        *output = input;
        return Status::OK();
    }
    ColumnProjects projects;
    for (size_t i = 0; i < input->GetOutputSchemaSourceSize(); ++i) {
        auto source = input->GetOutputSchemaSource(i);
        for (size_t j = 0; j < source->size(); ++j) {
            auto column =
                ctx->node_manager()->MakeColumnIdNode(source->GetColumnID(j));
            projects.Add(source->GetColumnName(j), column, nullptr);
        }
    }
    PhysicalSimpleProjectNode* op = nullptr;
    CHECK_STATUS(
        ctx->CreateOp<PhysicalSimpleProjectNode>(&op, input, projects));
    *output = op;
    return Status::OK();
}

Status CommonColumnOptimize::Apply(PhysicalPlanContext* ctx,
                                   PhysicalOpNode* input,
                                   PhysicalOpNode** output) {
    CHECK_TRUE(input != nullptr && output != nullptr, kPlanError);
    Init();
    BuildOpState* state = nullptr;
    CHECK_STATUS(GetOpState(ctx, input, &state));
    output_common_column_indices_ = state->common_column_indices;

    PhysicalOpNode* common_output = nullptr;
    CHECK_STATUS(GetSingleSliceResult(ctx, state->common_op, &common_output));

    PhysicalOpNode* non_common_output = nullptr;
    CHECK_STATUS(
        GetSingleSliceResult(ctx, state->non_common_op, &non_common_output));

    if (common_output == nullptr) {
        *output = non_common_output;
    } else if (non_common_output == nullptr) {
        *output = common_output;
    } else {
        PhysicalRequestJoinNode* concat = nullptr;
        CHECK_STATUS(ctx->CreateOp<PhysicalRequestJoinNode>(
            &concat, common_output, non_common_output, node::kJoinTypeConcat));
        *output = concat;
    }
    return Status::OK();
}

Status CommonColumnOptimize::ProcessRequest(
    PhysicalPlanContext* ctx, PhysicalRequestProviderNode* data_op,
    BuildOpState* state) {
    const codec::Schema* request_schema = data_op->GetOutputSchema();
    if (common_column_indices_.empty()) {
        state->SetAllNonCommon(data_op);
        return Status::OK();
    } else if (common_column_indices_.size() ==
               static_cast<size_t>(request_schema->size())) {
        state->SetAllCommon(data_op);
        return Status::OK();
    }

    // Create new request data op with common and non-common part
    PhysicalRequestProviderNodeWithCommonColumn* new_data_op = nullptr;
    CHECK_STATUS(ctx->CreateOp<PhysicalRequestProviderNodeWithCommonColumn>(
        &new_data_op, data_op->table_handler_, common_column_indices_));

    // Create common select
    ColumnProjects common_projects;
    auto common_source = new_data_op->GetOutputSchemaSource(0);
    for (size_t i = 0; i < common_source->size(); ++i) {
        size_t column_id = common_source->GetColumnID(i);
        const std::string& column_name = common_source->GetColumnName(i);
        auto col_ref = ctx->node_manager()->MakeColumnIdNode(column_id);
        common_projects.Add(column_name, col_ref, nullptr);
    }
    PhysicalSimpleProjectNode* common_select_op = nullptr;
    CHECK_STATUS(ctx->CreateOp<PhysicalSimpleProjectNode>(
        &common_select_op, new_data_op, common_projects));
    state->common_op = common_select_op;
    state->common_column_indices = common_column_indices_;

    // Create non-common select
    ColumnProjects non_common_projects;
    auto non_common_source = new_data_op->GetOutputSchemaSource(1);
    for (size_t i = 0; i < non_common_source->size(); ++i) {
        size_t column_id = non_common_source->GetColumnID(i);
        const std::string& column_name = non_common_source->GetColumnName(i);
        auto col_ref = ctx->node_manager()->MakeColumnIdNode(column_id);
        non_common_projects.Add(column_name, col_ref, nullptr);
    }
    PhysicalSimpleProjectNode* non_common_select_op = nullptr;
    CHECK_STATUS(ctx->CreateOp<PhysicalSimpleProjectNode>(
        &non_common_select_op, new_data_op, non_common_projects));
    state->non_common_op = non_common_select_op;
    return Status::OK();
}

Status CommonColumnOptimize::ProcessData(PhysicalPlanContext* ctx,
                                         PhysicalDataProviderNode* data_op,
                                         BuildOpState* state) {
    if (data_op->provider_type_ == DataProviderType::kProviderTypeRequest) {
        return ProcessRequest(
            ctx, dynamic_cast<PhysicalRequestProviderNode*>(data_op), state);
    } else {
        state->SetAllCommon(data_op);
        return Status::OK();
    }
}

/**
 * Check whether expression can be computed from left node
 */
static bool ExprDependOnlyOnLeft(const node::ExprNode* expr,
                                 PhysicalOpNode* left, PhysicalOpNode* right) {
    if (left == nullptr) {
        return false;
    }
    std::set<size_t> columns;
    Status status =
        left->schemas_ctx()->ResolveExprDependentColumns(expr, &columns);
    if (!status.isOK()) {
        return false;
    }
    if (right == nullptr) {
        return true;
    }
    status = right->schemas_ctx()->ResolveExprDependentColumns(expr, &columns);
    if (status.isOK()) {
        // if expr can also be resolved from right, return false
        // to ensure there is no ambiguousity.
        return false;
    } else {
        return true;
    }
}

template <typename Component>
static bool LeftDependOnlyOnPart(const Component& comp, PhysicalOpNode* part,
                                 PhysicalOpNode* origin_left,
                                 PhysicalOpNode* origin_right) {
    if (part == nullptr) {
        return false;
    }
    std::vector<const hybridse::node::ExprNode*> related_columns;
    comp.ResolvedRelatedColumns(&related_columns);
    for (auto col : related_columns) {
        // Check used column depend on left
        if (!ExprDependOnlyOnLeft(col, origin_left, origin_right)) {
            continue;
        }
        std::set<size_t> column_ids;
        if (!part->schemas_ctx()
                 ->ResolveExprDependentColumns(col, &column_ids)
                 .isOK()) {
            return false;
        }
    }
    return true;
}

static Status UpdateProjectExpr(
    PhysicalPlanContext* ctx, const node::ExprNode* expr,
    const PhysicalOpNode* origin_input,
    const std::unordered_map<size_t, size_t>& new_id_map,
    node::ExprNode** output) {
    auto schemas_ctx = origin_input->schemas_ctx();
    std::vector<const node::ExprNode*> origin_columns;
    CHECK_STATUS(
        schemas_ctx->ResolveExprDependentColumns(expr, &origin_columns));

    passes::ExprReplacer replacer;
    for (auto column_expr : origin_columns) {
        size_t schema_idx = 0;
        size_t col_idx = 0;
        if (column_expr->GetExprType() == node::kExprColumnId) {
            auto column_id =
                dynamic_cast<const node::ColumnIdNode*>(column_expr);
            CHECK_STATUS(schemas_ctx->ResolveColumnIndexByID(
                column_id->GetColumnID(), &schema_idx, &col_idx));
        } else if (column_expr->GetExprType() == node::kExprColumnRef) {
            auto column_ref =
                dynamic_cast<const node::ColumnRefNode*>(column_expr);
            CHECK_STATUS(schemas_ctx->ResolveColumnRefIndex(
                column_ref, &schema_idx, &col_idx));
        } else {
            return Status(kPlanError, "Illegal column expression");
        }
        size_t total_idx = col_idx;
        for (size_t i = 0; i < schema_idx; ++i) {
            total_idx += schemas_ctx->GetSchemaSource(i)->size();
        }
        auto iter = new_id_map.find(total_idx);
        CHECK_TRUE(iter != new_id_map.end(), kPlanError);
        size_t new_column_id = iter->second;
        auto new_column_expr =
            ctx->node_manager()->MakeColumnIdNode(new_column_id);
        replacer.AddReplacement(column_expr, new_column_expr);
    }
    return replacer.Replace(expr->DeepCopy(ctx->node_manager()), output);
}

static Status CreateSimplifiedProject(PhysicalPlanContext* ctx,
                                      PhysicalOpNode* input,
                                      const ColumnProjects& projects,
                                      PhysicalOpNode** output) {
    PhysicalOpNode* cur_input = input;
    bool can_project = true;
    while (can_project) {
        can_project = false;
        for (size_t i = 0; i < cur_input->producers().size(); ++i) {
            auto cand_input = cur_input->GetProducer(i);
            if (cand_input->GetOutputType() !=
                PhysicalSchemaType::kSchemaTypeRow) {
                continue;
            }
            bool is_valid = true;
            for (size_t j = 0; j < projects.size(); ++j) {
                if (!ExprDependOnlyOnLeft(projects.GetExpr(j), cand_input,
                                          nullptr)) {
                    is_valid = false;
                    break;
                }
            }
            if (is_valid) {
                can_project = true;
                cur_input = cand_input;
                break;
            }
        }
    }
    PhysicalSimpleProjectNode* project_op = nullptr;
    CHECK_STATUS(ctx->CreateOp<PhysicalSimpleProjectNode>(&project_op,
                                                          cur_input, projects));
    *output = project_op;
    return Status::OK();
}

Status CommonColumnOptimize::ProcessSimpleProject(
    PhysicalPlanContext* ctx, PhysicalSimpleProjectNode* project_op,
    BuildOpState* state) {
    // fetch input states
    BuildOpState* input_state = nullptr;
    auto input_op = project_op->GetProducer(0);
    CHECK_STATUS(GetOpState(ctx, input_op, &input_state));

    // maybe use concat input
    PhysicalOpNode* new_input = nullptr;
    CHECK_STATUS(GetConcatOp(ctx, input_op, &new_input));
    std::unordered_map<size_t, size_t> new_id_map;
    for (size_t i = 0; i < new_input->GetOutputSchemaSourceSize(); ++i) {
        auto source = new_input->GetOutputSchemaSource(i);
        for (size_t j = 0; j < source->size(); ++j) {
            auto col_idx = new_id_map.size();
            new_id_map[col_idx] = source->GetColumnID(j);
        }
    }

    // split project expressions
    ColumnProjects common_projects;
    ColumnProjects non_common_projects;
    bool use_concat_input = false;
    const ColumnProjects& origin_projects = project_op->project();
    for (size_t i = 0; i < origin_projects.size(); ++i) {
        const std::string& name = origin_projects.GetName(i);
        const node::ExprNode* expr = origin_projects.GetExpr(i);
        const node::FrameNode* frame = origin_projects.GetFrame(i);

        bool need_update = !ExprDependOnlyOnLeft(expr, new_input, nullptr);
        if (need_update) {
            node::ExprNode* new_expr = nullptr;
            CHECK_STATUS(
                UpdateProjectExpr(ctx, expr, input_op, new_id_map, &new_expr),
                "Fail to resolve expr ", expr->GetExprString(),
                " on project input:\n", new_input->SchemaToString(""));
            expr = new_expr;
        }

        if (ExprDependOnlyOnLeft(expr, input_state->common_op,
                                 input_state->non_common_op)) {
            state->AddCommonIdx(i);
            common_projects.Add(name, expr, frame);
        } else if (ExprDependOnlyOnLeft(expr, input_state->non_common_op,
                                        input_state->common_op)) {
            non_common_projects.Add(name, expr, frame);
        } else {
            use_concat_input = true;
            non_common_projects.Add(name, expr, frame);
        }
    }
    if (!use_concat_input && non_common_projects.size() > 0) {
        // expr depend on non-common part only
        new_input = input_state->non_common_op;
    }

    // create two new simple project ops
    if (common_projects.size() == origin_projects.size() &&
        input_op == input_state->common_op) {
        state->common_op = project_op;
    } else if (common_projects.size() > 0) {
        CHECK_STATUS(CreateSimplifiedProject(
            ctx, input_state->common_op, common_projects, &state->common_op))
    } else {
        state->common_op = nullptr;
    }

    if (non_common_projects.size() == origin_projects.size() &&
        input_op == new_input) {
        state->non_common_op = project_op;
    } else if (non_common_projects.size() > 0) {
        CHECK_STATUS(CreateSimplifiedProject(
            ctx, new_input, non_common_projects, &state->non_common_op))
    } else {
        state->non_common_op = nullptr;
    }
    return Status::OK();
}

static Status CreateNewProject(PhysicalPlanContext* ctx, ProjectType ptype,
                               PhysicalOpNode* input,
                               const ColumnProjects& projects,
                               const node::ExprNode* having_condition,
                               PhysicalOpNode** out) {
    switch (ptype) {
        case ProjectType::kRowProject: {
            PhysicalRowProjectNode* op = nullptr;
            CHECK_STATUS(
                ctx->CreateOp<PhysicalRowProjectNode>(&op, input, projects));
            *out = op;
            break;
        }
        case ProjectType::kTableProject: {
            PhysicalTableProjectNode* op = nullptr;
            CHECK_STATUS(
                ctx->CreateOp<PhysicalTableProjectNode>(&op, input, projects));
            *out = op;
            break;
        }
        case ProjectType::kAggregation: {
            PhysicalAggregationNode* op = nullptr;
            CHECK_STATUS(
                ctx->CreateOp<PhysicalAggregationNode>(&op, input, projects, having_condition));
            *out = op;
            break;
        }
        default:
            return Status(kPlanError,
                          "Unknown project type: " + ProjectTypeName(ptype));
    }
    return Status::OK();
}

Status CommonColumnOptimize::ProcessProject(PhysicalPlanContext* ctx,
                                            PhysicalProjectNode* project_op,
                                            BuildOpState* state) {
    // process window agg
    if (project_op->project_type_ == ProjectType::kAggregation) {
        auto window_agg_op = dynamic_cast<PhysicalAggregationNode*>(project_op);
        return ProcessWindow(ctx, window_agg_op, state);
    }

    // fetch input states
    auto input_op = project_op->GetProducer(0);
    BuildOpState* input_state = nullptr;
    CHECK_STATUS(GetOpState(ctx, input_op, &input_state));

    // maybe use concat input
    PhysicalOpNode* new_input = nullptr;
    CHECK_STATUS(GetConcatOp(ctx, input_op, &new_input));
    std::unordered_map<size_t, size_t> new_id_map;
    for (size_t i = 0; i < new_input->GetOutputSchemaSourceSize(); ++i) {
        auto source = new_input->GetOutputSchemaSource(i);
        for (size_t j = 0; j < source->size(); ++j) {
            auto col_idx = new_id_map.size();
            new_id_map[col_idx] = source->GetColumnID(j);
        }
    }

    // split project expressions
    ColumnProjects common_projects;
    ColumnProjects non_common_projects;
    bool use_concat_input = false;
    const ColumnProjects& origin_projects = project_op->project();
    for (size_t i = 0; i < origin_projects.size(); ++i) {
        const std::string& name = origin_projects.GetName(i);
        const node::ExprNode* expr = origin_projects.GetExpr(i);
        const node::FrameNode* frame = origin_projects.GetFrame(i);

        bool need_update = !ExprDependOnlyOnLeft(expr, new_input, nullptr);
        if (need_update) {
            node::ExprNode* new_expr = nullptr;
            CHECK_STATUS(
                UpdateProjectExpr(ctx, expr, input_op, new_id_map, &new_expr),
                "Fail to resolve expr ", expr->GetExprString(),
                " on project input:\n", new_input->SchemaToString(""));
            expr = new_expr;
        }

        if (ExprDependOnlyOnLeft(expr, input_state->common_op,
                                 input_state->non_common_op)) {
            state->AddCommonIdx(i);
            common_projects.Add(name, expr, frame);
        } else if (ExprDependOnlyOnLeft(expr, input_state->non_common_op,
                                        input_state->common_op)) {
            non_common_projects.Add(name, expr, frame);
        } else {
            use_concat_input = true;
            non_common_projects.Add(name, expr, frame);
        }
    }
    if (!use_concat_input && non_common_projects.size() > 0) {
        // expr depend on non-common part only
        new_input = input_state->non_common_op;
    }

    // Renew having condition if necessary
    node::ExprNode* new_having_condition = nullptr;
//    const node::ExprNode* having_condition = project_op->having_condition_.condition();
    vm::ConditionFilter having_condition;
    if (project_op->project_type_ == vm::kAggregation) {
        auto* agg_prj = dynamic_cast<vm::PhysicalAggregationNode*>(project_op);
        CHECK_TRUE(agg_prj != nullptr, kPlanError, "not a PhysicalAggregationNode");
        having_condition = agg_prj->having_condition_;
    } else if (project_op->project_type_ == vm::kGroupAggregation) {
        having_condition = dynamic_cast<vm::PhysicalGroupAggrerationNode*>(project_op)->having_condition_;
    }
    if (having_condition.ValidCondition()) {
        bool need_update =
            !ExprDependOnlyOnLeft(having_condition.condition(), new_input, nullptr);
        auto origin_having_condition = having_condition.condition()->ShadowCopy(ctx->node_manager());
        if (need_update) {
            passes::ExprReplacer replacer;
            CHECK_STATUS(BuildColumnReplacement(having_condition.condition(), new_input->schemas_ctx(),
                                                new_input->schemas_ctx(), ctx->node_manager(), &replacer));
            CHECK_STATUS(replacer.Replace(origin_having_condition, &new_having_condition));
        } else {
            new_having_condition = origin_having_condition;
        }
    }

    // create two new project ops
    if (common_projects.size() == origin_projects.size() &&
        input_state->common_op == input_op) {
        state->common_op = project_op;
    } else if (common_projects.size() > 0) {
        PhysicalOpNode* common_project_op = nullptr;
        CHECK_STATUS(CreateNewProject(ctx, project_op->project_type_,
                                      input_state->common_op, common_projects,
                                      new_having_condition,
                                      &common_project_op));
        state->common_op = common_project_op;
    } else {
        state->common_op = nullptr;
    }

    if (non_common_projects.size() == origin_projects.size() &&
        new_input == input_op) {
        state->non_common_op = project_op;
    } else if (non_common_projects.size() > 0) {
        PhysicalOpNode* non_common_project_op = nullptr;
        CHECK_STATUS(CreateNewProject(ctx, project_op->project_type_, new_input,
                                      non_common_projects,
                                      new_having_condition,
                                      &non_common_project_op));
        state->non_common_op = non_common_project_op;
    } else {
        state->non_common_op = nullptr;
    }
    return Status::OK();
}

Status CommonColumnOptimize::ProcessTrivial(PhysicalPlanContext* ctx,
                                            PhysicalOpNode* op,
                                            BuildOpState* state) {
    bool changed = false;
    bool is_common = true;
    std::vector<PhysicalOpNode*> children(op->producers().size(), nullptr);
    for (size_t i = 0; i < children.size(); ++i) {
        auto origin_child = op->GetProducer(i);
        CHECK_STATUS(GetReorderedOp(ctx, origin_child, &children[i]));
        if (children[i] != origin_child) {
            changed = true;
        }
        BuildOpState* child_state = &build_dict_[origin_child->node_id()];
        is_common &= child_state->non_common_op == nullptr;
    }
    if (changed) {
        PhysicalOpNode* new_op = nullptr;
        CHECK_STATUS(ctx->WithNewChildren(op, children, &new_op));
        op = new_op;
    }
    if (is_common) {
        state->SetAllCommon(op);
    } else {
        state->SetAllNonCommon(op);
    }
    return Status::OK();
}

static Status ReplaceRequestUnion(PhysicalPlanContext* ctx,
                                  PhysicalOpNode* root,
                                  PhysicalRequestUnionNode* target,
                                  PhysicalOpNode* repl,
                                  std::map<size_t, PhysicalOpNode*>* visited,
                                  PhysicalOpNode** output) {
    // check visisted
    auto iter = visited->find(root->node_id());
    if (iter != visited->end()) {
        *output = iter->second;
        return Status::OK();
    }
    if (root->GetOpType() == PhysicalOpType::kPhysicalOpRequestUnion) {
        auto request_union_op = dynamic_cast<PhysicalRequestUnionNode*>(root);
        CHECK_TRUE(request_union_op == target, kPlanError,
                   "Found another request union node under window agg");
        *output = repl;
        (*visited)[root->node_id()] = repl;
        return Status::OK();
    }
    bool changed = false;
    std::vector<PhysicalOpNode*> new_children(root->GetProducerCnt());
    for (size_t i = 0; i < root->GetProducerCnt(); ++i) {
        CHECK_STATUS(ReplaceRequestUnion(ctx, root->GetProducer(i), target,
                                         repl, visited, &new_children[i]));
        if (new_children[i] != root->GetProducer(i)) {
            changed = true;
        }
    }
    if (changed) {
        PhysicalOpNode* new_root = nullptr;
        CHECK_STATUS(ctx->WithNewChildren(root, new_children, &new_root));
        if (new_root->GetOpType() == PhysicalOpType::kPhysicalOpJoin &&
            !new_children.empty() &&
            new_children[0]->GetOutputType() ==
                PhysicalSchemaType::kSchemaTypeRow) {
            auto join_op = dynamic_cast<PhysicalJoinNode*>(new_root);
            PhysicalRequestJoinNode* request_join_op = nullptr;
            CHECK_STATUS(ctx->CreateOp<PhysicalRequestJoinNode>(
                &request_join_op, new_children[0], new_children[1],
                join_op->join(), join_op->output_right_only()));
            *output = request_join_op;
        } else {
            *output = new_root;
        }
    } else {
        *output = root;
    }
    (*visited)[root->node_id()] = *output;
    return Status::OK();
}

Status CommonColumnOptimize::ProcessRequestUnion(
    PhysicalPlanContext* ctx, PhysicalRequestUnionNode* request_union_op,
    const std::vector<PhysicalOpNode*>& path, PhysicalOpNode** out,
    BuildOpState** agg_request_state) {
    // get request row
    BuildOpState* request_state = nullptr;
    auto origin_input_op = request_union_op->GetProducer(0);
    CHECK_STATUS(GetOpState(ctx, origin_input_op, &request_state));
    CHECK_TRUE(request_state->common_op != nullptr, kPlanError);

    // get common request unions
    PhysicalOpNode* new_right = nullptr;
    CHECK_STATUS(
        GetReorderedOp(ctx, request_union_op->GetProducer(1), &new_right));

    // create common request union op which do not output request row
    PhysicalRequestUnionNode* new_request_union = nullptr;
    CHECK_STATUS(ctx->CreateOp<PhysicalRequestUnionNode>(
        &new_request_union, request_state->common_op, new_right,
        request_union_op->window(), request_union_op->instance_not_in_window(),
        request_union_op->exclude_current_time(), false));
    SetAllCommon(new_request_union);

    for (auto& pair : request_union_op->window_unions().window_unions_) {
        PhysicalOpNode* new_right_union = nullptr;
        CHECK_STATUS(GetReorderedOp(ctx, pair.first, &new_right_union));
        new_request_union->AddWindowUnion(new_right_union);

        // reset window spec
        auto window_ptr =
            &new_request_union->window_unions_.window_unions_.back().second;
        CHECK_STATUS(
            ReplaceComponentExpr(pair.second, origin_input_op->schemas_ctx(),
                                 request_state->common_op->schemas_ctx(),
                                 ctx->node_manager(), window_ptr));
    }

    // apply ops path to window part
    std::map<size_t, PhysicalOpNode*> replace_cache;
    PhysicalOpNode* window_root = nullptr;
    CHECK_STATUS(ReplaceRequestUnion(ctx, path[0], request_union_op,
                                     new_request_union, &replace_cache,
                                     &window_root));
    SetAllCommon(window_root);
    PhysicalOpNode* optimized_window_root = nullptr;
    CHECK_STATUS(GetReorderedOp(ctx, window_root, &optimized_window_root));

    // apply ops path to request part
    replace_cache.clear();
    PhysicalOpNode* request_root = nullptr;
    CHECK_STATUS(ReplaceRequestUnion(ctx, path[0], request_union_op,
                                     origin_input_op, &replace_cache,
                                     &request_root));
    PhysicalOpNode* optimized_request_root = nullptr;
    CHECK_STATUS(GetOpState(ctx, request_root, agg_request_state));
    CHECK_STATUS(GetReorderedOp(ctx, request_root, &optimized_request_root));

    // final union
    Range request_ts;
    CHECK_STATUS(ReplaceComponentExpr(request_union_op->window().range_,
                                      origin_input_op->schemas_ctx(),
                                      request_state->common_op->schemas_ctx(),
                                      ctx->node_manager(), &request_ts));
    PhysicalPostRequestUnionNode* union_op = nullptr;
    CHECK_STATUS(ctx->CreateOp<PhysicalPostRequestUnionNode>(
        &union_op, optimized_request_root, optimized_window_root, request_ts));
    *out = union_op;
    return Status::OK();
}

Status CommonColumnOptimize::ProcessWindow(PhysicalPlanContext* ctx,
                                           PhysicalAggregationNode* agg_op,
                                           BuildOpState* state) {
    // find request union path
    auto input = agg_op->GetProducer(0);
    std::vector<PhysicalOpNode*> request_union_path;
    bool found = FindRequestUnionPath(input, &request_union_path);
    if (!found || request_union_path.empty()) {
        return ProcessTrivial(ctx, agg_op, state);
    }

    auto request_union_op =
        dynamic_cast<PhysicalRequestUnionNode*>(request_union_path.back());
    BuildOpState* request_state = nullptr;
    CHECK_STATUS(
        GetOpState(ctx, request_union_op->GetProducer(0), &request_state));

    // process request union path
    bool union_is_non_trivial_common =
        request_state->common_op != nullptr &&      // input has common part
        request_state->non_common_op != nullptr &&  // input is non-trivial
        LeftDependOnlyOnPart(request_union_op->window(),
                             request_state->common_op,
                             request_union_op->GetProducer(0), nullptr);
    for (auto& pair : request_union_op->window_unions_.window_unions_) {
        union_is_non_trivial_common &=
            LeftDependOnlyOnPart(pair.second, request_state->common_op,
                                 request_union_op->GetProducer(0), nullptr);
    }

    PhysicalOpNode* new_union = nullptr;
    BuildOpState* agg_request_state = nullptr;
    if (union_is_non_trivial_common) {
        CHECK_STATUS(ProcessRequestUnion(ctx, request_union_op,
                                         request_union_path, &new_union,
                                         &agg_request_state));
    } else {
        CHECK_STATUS(GetOpState(ctx, input, &agg_request_state));
        CHECK_STATUS(GetReorderedOp(ctx, input, &new_union));
    }
    bool is_trival_common =
        !union_is_non_trivial_common && request_state->non_common_op == nullptr;

    // collect new agg request input column ids
    PhysicalOpNode* agg_request_concat = agg_request_state->concat_op;
    CHECK_TRUE(agg_request_concat != nullptr, kPlanError);
    std::unordered_map<size_t, size_t> new_id_map;
    for (size_t i = 0; i < agg_request_concat->GetOutputSchemaSourceSize();
         ++i) {
        auto source = agg_request_concat->GetOutputSchemaSource(i);
        for (size_t j = 0; j < source->size(); ++j) {
            auto col_idx = new_id_map.size();
            new_id_map[col_idx] = source->GetColumnID(j);
        }
    }

    // split project expressions
    ColumnProjects common_projects;
    ColumnProjects non_common_projects;
    const ColumnProjects& origin_projects = agg_op->project();
    for (size_t i = 0; i < origin_projects.size(); ++i) {
        const std::string& name = origin_projects.GetName(i);
        const node::ExprNode* expr = origin_projects.GetExpr(i);
        const node::FrameNode* frame = origin_projects.GetFrame(i);

        bool need_update =
            !ExprDependOnlyOnLeft(expr, agg_request_concat, nullptr);
        if (need_update) {
            node::ExprNode* new_expr = nullptr;
            CHECK_STATUS(
                UpdateProjectExpr(ctx, expr, input, new_id_map, &new_expr),
                "Fail to resolve expr ", expr->GetExprString(),
                " on project input:\n", new_union->SchemaToString(""));
            expr = new_expr;
        }
        bool expr_is_common =
            is_trival_common || agg_request_state->non_common_op == nullptr ||
            (union_is_non_trivial_common &&
             ExprDependOnlyOnLeft(expr, agg_request_state->common_op,
                                  agg_request_state->non_common_op));
        if (expr_is_common) {
            state->AddCommonIdx(i);
            common_projects.Add(name, expr, frame);
        } else {
            non_common_projects.Add(name, expr, frame);
        }
    }

    // Renew having condition if necessary
    node::ExprNode* new_having_condition = nullptr;
    if (agg_op->having_condition_.ValidCondition()) {
        const node::ExprNode* having_condition = agg_op->having_condition_.condition();
        bool need_update =
            !ExprDependOnlyOnLeft(having_condition, agg_request_concat, nullptr);
        auto origin_having_condition = having_condition->ShadowCopy(ctx->node_manager());
        if (need_update) {
            passes::ExprReplacer replacer;
            CHECK_STATUS(BuildColumnReplacement(having_condition, agg_request_concat->schemas_ctx(),
                                                agg_request_concat->schemas_ctx(), ctx->node_manager(), &replacer));
            CHECK_STATUS(replacer.Replace(origin_having_condition, &new_having_condition));
        } else {
            new_having_condition = origin_having_condition;
        }
    }
    // create two new project ops
    if (common_projects.size() == origin_projects.size() &&
        new_union == agg_op->GetProducer(0)) {
        state->common_op = agg_op;
    } else if (common_projects.size() > 0) {
        PhysicalAggregationNode* common_project_op = nullptr;
        CHECK_STATUS(ctx->CreateOp<PhysicalAggregationNode>(
            &common_project_op, new_union, common_projects, new_having_condition));
        state->common_op = common_project_op;
    } else {
        state->common_op = nullptr;
    }

    if (non_common_projects.size() == origin_projects.size() &&
        new_union == agg_op->GetProducer(0)) {
        state->non_common_op = agg_op;
    } else if (non_common_projects.size() > 0) {
        PhysicalAggregationNode* non_common_project_op = nullptr;
        CHECK_STATUS(ctx->CreateOp<PhysicalAggregationNode>(
            &non_common_project_op, new_union, non_common_projects, new_having_condition));
        state->non_common_op = non_common_project_op;
    } else {
        state->non_common_op = nullptr;
    }
    return Status::OK();
}

Status CommonColumnOptimize::ProcessConcat(PhysicalPlanContext* ctx,
                                           PhysicalRequestJoinNode* input,
                                           BuildOpState* state) {
    BuildOpState* left_state = nullptr;
    CHECK_STATUS(GetOpState(ctx, input->GetProducer(0), &left_state));

    BuildOpState* right_state = nullptr;
    CHECK_STATUS(GetOpState(ctx, input->GetProducer(1), &right_state));

    // build common
    if (left_state->common_op == nullptr) {
        state->common_op = right_state->common_op;
    } else if (right_state->common_op == nullptr) {
        state->common_op = left_state->common_op;
    } else {
        PhysicalRequestJoinNode* concat_op = nullptr;
        CHECK_STATUS(ctx->CreateOp<PhysicalRequestJoinNode>(
            &concat_op, left_state->common_op, right_state->common_op,
            node::kJoinTypeConcat));
        state->common_op = concat_op;
    }

    // build non-common
    if (left_state->non_common_op == nullptr) {
        state->non_common_op = right_state->non_common_op;
    } else if (right_state->non_common_op == nullptr) {
        state->non_common_op = left_state->non_common_op;
    } else {
        PhysicalRequestJoinNode* concat_op = nullptr;
        CHECK_STATUS(ctx->CreateOp<PhysicalRequestJoinNode>(
            &concat_op, left_state->non_common_op, right_state->non_common_op,
            node::kJoinTypeConcat));
        state->non_common_op = concat_op;
    }

    // set common indices
    for (size_t left_idx : left_state->common_column_indices) {
        state->AddCommonIdx(left_idx);
    }
    for (size_t right_idx : right_state->common_column_indices) {
        state->AddCommonIdx(right_idx +
                            input->GetProducer(0)->GetOutputSchemaSize());
    }

    CHECK_TRUE(state->IsInitialized(), kPlanError);
    return Status::OK();
}

Status CommonColumnOptimize::ProcessRename(PhysicalPlanContext* ctx,
                                           PhysicalRenameNode* input,
                                           BuildOpState* state) {
    BuildOpState* input_state = nullptr;
    CHECK_STATUS(GetOpState(ctx, input->GetProducer(0), &input_state));

    const std::string& alias_name = input->name_;
    state->common_column_indices = input_state->common_column_indices;

    if (input_state->common_op != nullptr &&
        input_state->non_common_op != nullptr) {
        PhysicalRenameNode* common_rename_op = nullptr;
        CHECK_STATUS(ctx->CreateOp<PhysicalRenameNode>(
            &common_rename_op, input_state->common_op, alias_name));
        state->common_op = common_rename_op;

        PhysicalRenameNode* non_common_rename_op = nullptr;
        CHECK_STATUS(ctx->CreateOp<PhysicalRenameNode>(
            &non_common_rename_op, input_state->non_common_op, alias_name));
        state->non_common_op = non_common_rename_op;
    } else {
        CHECK_STATUS(ProcessTrivial(ctx, input, state));
    }
    return Status::OK();
}

Status CommonColumnOptimize::ProcessJoin(PhysicalPlanContext* ctx,
                                         PhysicalRequestJoinNode* join_op,
                                         BuildOpState* state) {
    // process concat
    if (join_op->join().join_type() == node::kJoinTypeConcat) {
        return ProcessConcat(ctx, join_op, state);
    }

    // fetch left inputs
    BuildOpState* left_state = nullptr;
    CHECK_STATUS(GetOpState(ctx, join_op->GetProducer(0), &left_state));

    // fetch right input
    // right side of request join do not consider splits
    PhysicalOpNode* right = nullptr;
    CHECK_STATUS(GetConcatOp(ctx, join_op->GetProducer(1), &right));

    // check whether common or non-common join
    bool is_common_join =
        LeftDependOnlyOnPart(join_op->join(), left_state->common_op,
                             join_op->GetProducer(0), join_op->GetProducer(1));
    bool is_non_common_join =
        LeftDependOnlyOnPart(join_op->join(), left_state->non_common_op,
                             join_op->GetProducer(0), join_op->GetProducer(1));

    if (is_common_join) {
        // join only depend on common left part
        // set common indices
        for (size_t left_idx : left_state->common_column_indices) {
            state->AddCommonIdx(left_idx);
        }
        for (size_t i = 0; i < right->GetOutputSchemaSize(); ++i) {
            state->AddCommonIdx(join_op->GetProducer(0)->GetOutputSchemaSize() +
                                i);
        }

        if (left_state->common_op == join_op->GetProducer(0) &&
            right == join_op->GetProducer(1)) {
            state->common_op = join_op;
            state->non_common_op = nullptr;
        } else {
            PhysicalRequestJoinNode* new_join = nullptr;
            CHECK_STATUS(ctx->CreateOp<PhysicalRequestJoinNode>(
                &new_join, left_state->common_op, right, join_op->join(),
                join_op->output_right_only()));
            CHECK_STATUS(ReplaceComponentExpr(
                join_op->join(), join_op->joined_schemas_ctx(),
                new_join->joined_schemas_ctx(), ctx->node_manager(),
                &new_join->join_));
            state->common_op = new_join;
            state->non_common_op = join_op->output_right_only()
                                       ? nullptr
                                       : left_state->non_common_op;
        }
    } else if (is_non_common_join) {
        // join only depend on non-common left part
        if (left_state->non_common_op == join_op->GetProducer(0) &&
            right == join_op->GetProducer(1)) {
            state->common_op = nullptr;
            state->non_common_op = join_op;
        } else {
            PhysicalRequestJoinNode* new_join = nullptr;
            CHECK_STATUS(ctx->CreateOp<PhysicalRequestJoinNode>(
                &new_join, left_state->non_common_op, right, join_op->join(),
                join_op->output_right_only()));
            CHECK_STATUS(ReplaceComponentExpr(
                join_op->join(), join_op->joined_schemas_ctx(),
                new_join->joined_schemas_ctx(), ctx->node_manager(),
                &new_join->join_));
            state->common_op =
                join_op->output_right_only() ? nullptr : left_state->common_op;
            state->non_common_op = new_join;
            if (!join_op->output_right_only()) {
                for (size_t left_idx : left_state->common_column_indices) {
                    state->AddCommonIdx(left_idx);
                }
            }
        }
    } else {
        // join depend on concated left
        PhysicalOpNode* concat_left = nullptr;
        CHECK_STATUS(GetConcatOp(ctx, join_op->GetProducer(0), &concat_left));

        PhysicalRequestJoinNode* new_join = nullptr;
        CHECK_STATUS(ctx->CreateOp<PhysicalRequestJoinNode>(
            &new_join, concat_left, right, join_op->join(), true));
        CHECK_STATUS(
            ReplaceComponentExpr(join_op->join(), join_op->joined_schemas_ctx(),
                                 new_join->joined_schemas_ctx(),
                                 ctx->node_manager(), &new_join->join_));

        if (join_op->output_right_only()) {
            state->common_op = nullptr;
            state->non_common_op = new_join;
        } else {
            PhysicalRequestJoinNode* concat_non_common = nullptr;
            CHECK_STATUS(ctx->CreateOp<PhysicalRequestJoinNode>(
                &concat_non_common, left_state->non_common_op, new_join,
                node::kJoinTypeConcat));
            state->common_op = left_state->common_op;
            state->non_common_op = concat_non_common;
            for (size_t left_idx : left_state->common_column_indices) {
                state->AddCommonIdx(left_idx);
            }
        }
    }
    return Status::OK();
}

Status CommonColumnOptimize::GetOpState(PhysicalPlanContext* ctx,
                                        PhysicalOpNode* input,
                                        BuildOpState** state_ptr) {
    // lookup cache
    CHECK_TRUE(input != nullptr, kPlanError);
    BuildOpState* state = &build_dict_[input->node_id()];
    if (state->IsInitialized()) {
        *state_ptr = state;
        return Status::OK();
    }
    switch (input->GetOpType()) {
        case PhysicalOpType::kPhysicalOpDataProvider: {
            auto data_op = dynamic_cast<PhysicalDataProviderNode*>(input);
            CHECK_STATUS(ProcessData(ctx, data_op, state));
            break;
        }
        case PhysicalOpType::kPhysicalOpSimpleProject: {
            auto project_op = dynamic_cast<PhysicalSimpleProjectNode*>(input);
            CHECK_STATUS(ProcessSimpleProject(ctx, project_op, state));
            break;
        }
        case PhysicalOpType::kPhysicalOpProject: {
            auto project_op = dynamic_cast<PhysicalProjectNode*>(input);
            CHECK_STATUS(ProcessProject(ctx, project_op, state));
            break;
        }
        case PhysicalOpType::kPhysicalOpRequestJoin: {
            auto join_op = dynamic_cast<PhysicalRequestJoinNode*>(input);
            CHECK_STATUS(ProcessJoin(ctx, join_op, state));
            break;
        }
        case PhysicalOpType::kPhysicalOpRename: {
            auto rename_op = dynamic_cast<PhysicalRenameNode*>(input);
            CHECK_STATUS(ProcessRename(ctx, rename_op, state));
            break;
        }
        default: {
            CHECK_STATUS(ProcessTrivial(ctx, input, state));
            break;
        }
    }
    CHECK_TRUE(state->IsInitialized(), kPlanError);
    // set limit
    if (state->common_op != nullptr) {
        state->common_op->SetLimitCnt(input->GetLimitCnt());
    }
    if (state->non_common_op != nullptr) {
        state->non_common_op->SetLimitCnt(input->GetLimitCnt());
    }
    *state_ptr = state;
    return Status::OK();
}

Status CommonColumnOptimize::GetConcatOp(PhysicalPlanContext* ctx,
                                         PhysicalOpNode* input,
                                         PhysicalOpNode** out) {
    BuildOpState* state = nullptr;
    CHECK_STATUS(GetOpState(ctx, input, &state));
    if (state->concat_op != nullptr) {
        *out = state->concat_op;
        return Status::OK();
    }
    PhysicalOpNode* concat_op = nullptr;
    if (state->common_op == nullptr) {
        CHECK_TRUE(state->non_common_op != nullptr, kPlanError);
        concat_op = state->non_common_op;
    } else if (state->non_common_op == nullptr) {
        concat_op = state->common_op;
    } else {
        PhysicalRequestJoinNode* join = nullptr;
        CHECK_STATUS(ctx->CreateOp<PhysicalRequestJoinNode>(
            &join, state->common_op, state->non_common_op,
            node::kJoinTypeConcat));
        concat_op = join;
    }

    concat_op->SetLimitCnt(input->GetLimitCnt());
    state->concat_op = concat_op;
    *out = concat_op;
    return Status::OK();
}

Status CommonColumnOptimize::GetReorderedOp(PhysicalPlanContext* ctx,
                                            PhysicalOpNode* input,
                                            PhysicalOpNode** out) {
    BuildOpState* state = nullptr;
    CHECK_STATUS(GetOpState(ctx, input, &state));
    if (state->reordered_op != nullptr) {
        *out = state->reordered_op;
        return Status::OK();
    }
    size_t common_size = state->common_op != nullptr
                             ? state->common_op->GetOutputSchemaSize()
                             : 0;

    // get concat op
    PhysicalOpNode* concat_op = nullptr;
    CHECK_STATUS(GetConcatOp(ctx, input, &concat_op));
    // check whether reorder is required
    // reordered op should take same slice num and sizes with
    // original op and the column order is same
    bool need_reorder = false;
    if (input->GetOutputSchemaSourceSize() !=
        concat_op->GetOutputSchemaSourceSize()) {
        need_reorder = true;
    } else {
        for (size_t i = 0; i < input->GetOutputSchemaSourceSize(); ++i) {
            if (input->GetOutputSchemaSource(i)->size() !=
                concat_op->GetOutputSchemaSource(i)->size()) {
                need_reorder = true;
                break;
            }
        }
    }
    for (size_t i = 0; i < state->common_column_indices.size(); ++i) {
        if (state->common_column_indices.find(i) ==
            state->common_column_indices.end()) {
            need_reorder = true;
            break;
        }
    }
    if (!need_reorder) {
        state->reordered_op = concat_op;
        *out = concat_op;
        return Status::OK();
    }

    // get column id by total column index
    auto get_column_id = [](PhysicalOpNode* op, size_t total_idx) -> size_t {
        size_t cur_idx = total_idx;
        for (size_t i = 0; i < op->GetOutputSchemaSourceSize(); ++i) {
            auto source = op->GetOutputSchemaSource(i);
            if (cur_idx < source->size()) {
                return source->GetColumnID(cur_idx);
            }
            cur_idx -= source->size();
        }
        LOG(WARNING) << "Column index out of bound: " << total_idx;
        return 0;
    };

    PhysicalOpNode* reordered_op = nullptr;
    size_t cur_common_idx = 0;
    size_t cur_non_common_idx = 0;
    size_t cur_source_idx = 0;
    auto cur_source = input->GetOutputSchemaSource(cur_source_idx);
    ColumnProjects cur_projects;
    for (size_t i = 0; i < input->GetOutputSchemaSize(); ++i) {
        // create simple select column
        size_t select_column_id;
        auto iter = state->common_column_indices.find(i);
        if (iter != state->common_column_indices.end()) {
            select_column_id = get_column_id(concat_op, cur_common_idx);
            cur_common_idx += 1;
        } else {
            select_column_id =
                get_column_id(concat_op, common_size + cur_non_common_idx);
            cur_non_common_idx += 1;
        }
        const std::string& col_name = input->GetOutputSchema()->Get(i).name();
        auto col_ref = ctx->node_manager()->MakeColumnIdNode(select_column_id);
        cur_projects.Add(col_name, col_ref, nullptr);

        // output reordered slice
        if (cur_projects.size() == cur_source->size()) {
            PhysicalSimpleProjectNode* select_op = nullptr;
            CHECK_STATUS(ctx->CreateOp<PhysicalSimpleProjectNode>(
                &select_op, concat_op, cur_projects));
            if (reordered_op == nullptr) {
                reordered_op = select_op;
            } else {
                PhysicalRequestJoinNode* next_concat = nullptr;
                CHECK_STATUS(ctx->CreateOp<PhysicalRequestJoinNode>(
                    &next_concat, reordered_op, select_op,
                    node::kJoinTypeConcat));
                reordered_op = next_concat;
            }
            cur_projects.Clear();
            cur_source_idx += 1;
            if (cur_source_idx < input->GetOutputSchemaSourceSize()) {
                cur_source = input->GetOutputSchemaSource(cur_source_idx);
            }
        }
    }
    CHECK_TRUE(reordered_op != nullptr && reordered_op->GetOutputSchemaSize() ==
                                              input->GetOutputSchemaSize(),
               kPlanError);
    reordered_op->SetLimitCnt(input->GetLimitCnt());
    state->reordered_op = reordered_op;
    *out = reordered_op;
    return Status::OK();
}

void CommonColumnOptimize::SetAllCommon(PhysicalOpNode* op) {
    auto state = &this->build_dict_[op->node_id()];
    state->SetAllCommon(op);
}

void CommonColumnOptimize::ExtractCommonNodeSet(std::set<size_t>* output) {
    for (auto& pair : build_dict_) {
        auto& state = pair.second;
        if (state.common_op != nullptr) {
            output->insert(state.common_op->node_id());
        }
    }
}

}  // namespace passes
}  // namespace hybridse
