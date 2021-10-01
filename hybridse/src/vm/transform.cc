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

#include "vm/transform.h"
#include <map>
#include <set>
#include <stack>
#include <unordered_map>
#include "codegen/context.h"
#include "codegen/fn_ir_builder.h"
#include "codegen/fn_let_ir_builder.h"
#include "passes/physical/transform_up_physical_pass.h"
#include "vm/physical_op.h"
#include "vm/schemas_context.h"

#include "passes/expression/expr_pass.h"
#include "passes/lambdafy_projects.h"
#include "passes/physical/batch_request_optimize.h"
#include "passes/physical/cluster_optimized.h"
#include "passes/physical/condition_optimized.h"
#include "passes/physical/group_and_sort_optimized.h"
#include "passes/physical/left_join_optimized.h"
#include "passes/physical/limit_optimized.h"
#include "passes/physical/simple_project_optimized.h"
#include "passes/physical/window_column_pruning.h"
#include "passes/resolve_fn_and_attrs.h"

using ::hybridse::base::Status;
using ::hybridse::common::kPlanError;
using ::hybridse::common::kCodegenError;

namespace hybridse {
namespace vm {

using hybridse::passes::CheckExprDependOnChildOnly;
using hybridse::passes::ClusterOptimized;
using hybridse::passes::CommonColumnOptimize;
using hybridse::passes::ConditionOptimized;
using hybridse::passes::GroupAndSortOptimized;
using hybridse::passes::LeftJoinOptimized;
using hybridse::passes::LimitOptimized;
using hybridse::passes::PhysicalPlanPassType;
using hybridse::passes::SimpleProjectOptimized;
using hybridse::passes::WindowColumnPruning;

std::ostream& operator<<(std::ostream& output,
                         const hybridse::vm::LogicalOp& thiz) {
    return output << *(thiz.node_);
}

BatchModeTransformer::BatchModeTransformer(node::NodeManager* node_manager, const std::string& db,
                                           const std::shared_ptr<Catalog>& catalog,
                                           const codec::Schema* parameter_types, ::llvm::Module* module,
                                           const udf::UdfLibrary* library)
    : node_manager_(node_manager),
      db_(db),
      catalog_(catalog),
      module_(module),
      id_(0),
      performance_sensitive_mode_(false),
      cluster_optimized_mode_(false),
      enable_batch_window_parallelization_(false),
      library_(library),
      plan_ctx_(node_manager, library, db, catalog, parameter_types, false) {}

BatchModeTransformer::BatchModeTransformer(node::NodeManager* node_manager, const std::string& db,
                                           const std::shared_ptr<Catalog>& catalog,
                                           const codec::Schema* parameter_types, ::llvm::Module* module,
                                           const udf::UdfLibrary* library, bool performance_sensitive,
                                           bool cluster_optimized_mode, bool enable_expr_opt,
                                           bool enable_window_parallelization)
    : node_manager_(node_manager),
      db_(db),
      catalog_(catalog),
      module_(module),
      id_(0),
      performance_sensitive_mode_(performance_sensitive),
      cluster_optimized_mode_(cluster_optimized_mode),
      enable_batch_window_parallelization_(enable_window_parallelization),
      library_(library),
      plan_ctx_(node_manager, library, db, catalog, parameter_types, enable_expr_opt) {}

BatchModeTransformer::~BatchModeTransformer() {}

Status BatchModeTransformer::TransformPlanOp(const node::PlanNode* node, PhysicalOpNode** output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError, "Input node or output node is null");

    LogicalOp logical_op = LogicalOp(node);
    auto map_iter = op_map_.find(logical_op);
    // logical plan node already exist
    if (map_iter != op_map_.cend()) {
        *output = map_iter->second;
        return Status::OK();
    }

    Status status;
    ::hybridse::vm::PhysicalOpNode* op = nullptr;
    switch (node->type_) {
        case node::kPlanTypeLimit: {
            CHECK_STATUS(TransformLimitOp(
                dynamic_cast<const ::hybridse::node::LimitPlanNode*>(node),
                &op));
            break;
        }
        case node::kPlanTypeProject: {
            CHECK_STATUS(TransformProjectPlanOp(
                dynamic_cast<const ::hybridse::node::ProjectPlanNode*>(node),
                &op));
            break;
        }
        case node::kPlanTypeJoin: {
            CHECK_STATUS(TransformJoinOp(
                dynamic_cast<const ::hybridse::node::JoinPlanNode*>(node), &op));
            break;
        }
        case node::kPlanTypeUnion: {
            CHECK_STATUS(TransformUnionOp(dynamic_cast<const ::hybridse::node::UnionPlanNode*>(node), &op));
            break;
        }
        case node::kPlanTypeGroup: {
            CHECK_STATUS(TransformGroupOp(
                dynamic_cast<const ::hybridse::node::GroupPlanNode*>(node),
                &op));
            break;
        }
        case node::kPlanTypeSort: {
            CHECK_STATUS(TransformSortOp(
                dynamic_cast<const ::hybridse::node::SortPlanNode*>(node), &op));
            break;
        }
        case node::kPlanTypeFilter: {
            CHECK_STATUS(TransformFilterOp(
                dynamic_cast<const ::hybridse::node::FilterPlanNode*>(node),
                &op));
            break;
        }
        case node::kPlanTypeTable: {
            CHECK_STATUS(TransformScanOp(
                dynamic_cast<const ::hybridse::node::TablePlanNode*>(node),
                &op));
            break;
        }
        case node::kPlanTypeQuery: {
            CHECK_STATUS(TransformQueryPlan(
                dynamic_cast<const ::hybridse::node::QueryPlanNode*>(node),
                &op));
            break;
        }
        case node::kPlanTypeRename: {
            CHECK_STATUS(TransformRenameOp(
                dynamic_cast<const ::hybridse::node::RenamePlanNode*>(node),
                &op));
            break;
        }
        case node::kPlanTypeDistinct: {
            CHECK_STATUS(TransformDistinctOp(
                dynamic_cast<const ::hybridse::node::DistinctPlanNode*>(node),
                &op));
            break;
        }
        default: {
            FAIL_STATUS(kPlanError,
                        "Fail to transform physical plan: "
                        "can't handle type ",
                        node::NameOfPlanNodeType(node->type_));
        }
    }
    op_map_[logical_op] = op;
    *output = op;
    return base::Status::OK();
}

Status BatchModeTransformer::InitFnInfo(PhysicalOpNode* node,
                                        std::set<PhysicalOpNode*>* visited) {
    auto visited_iter = visited->find(node);
    if (visited_iter != visited->end()) {
        return Status::OK();
    } else {
        visited->insert(visited_iter, node);
    }
    CHECK_TRUE(node != nullptr, kPlanError, "Input node is null");
    for (auto producer : node->producers()) {
        // gen for producers recursively
        CHECK_STATUS(InitFnInfo(producer, visited));
    }
    switch (node->GetOpType()) {
        case kPhysicalOpGroupBy: {
            auto group_op = dynamic_cast<PhysicalGroupNode*>(node);
            CHECK_STATUS(
                GenKey(&group_op->group_, node->producers()[0]->schemas_ctx()));
            break;
        }
        case kPhysicalOpSortBy: {
            auto sort_op = dynamic_cast<PhysicalSortNode*>(node);
            CHECK_STATUS(
                GenSort(&sort_op->sort_, node->producers()[0]->schemas_ctx()));
            break;
        }
        case kPhysicalOpProject: {
            auto project_op = dynamic_cast<PhysicalProjectNode*>(node);
            if (kWindowAggregation != project_op->project_type_) {
                break;
            }
            auto window_agg_op =
                dynamic_cast<PhysicalWindowAggrerationNode*>(node);
            CHECK_STATUS(GenWindow(&window_agg_op->window_,
                                   window_agg_op->producers()[0]));
            for (auto& window_union :
                 window_agg_op->window_unions_.window_unions_) {
                CHECK_STATUS(InitFnInfo(window_union.first, visited),
                             "Fail Gen Window Union Sub Query Plan");
            }
            CHECK_STATUS(GenWindowUnionList(&window_agg_op->window_unions_,
                                            window_agg_op->producers()[0]));
            for (auto& pair : window_agg_op->window_joins_.window_joins()) {
                CHECK_STATUS(InitFnInfo(pair.first, visited),
                             "Fail Gen Window Join Sub Query Plan");
            }
            CHECK_STATUS(GenWindowJoinList(window_agg_op,
                                           window_agg_op->producers()[0]));
            break;
        }
        case kPhysicalOpSimpleProject:
        case kPhysicalOpConstProject: {
            break;
        }
        case kPhysicalOpFilter: {
            auto op = dynamic_cast<PhysicalFilterNode*>(node);
            CHECK_STATUS(GenFilter(&op->filter_, node->producers()[0]));
            break;
        }
        case kPhysicalOpJoin: {
            auto op = dynamic_cast<PhysicalJoinNode*>(node);
            CHECK_STATUS(GenJoin(&op->join_, node));
            break;
        }
        case kPhysicalOpRequestJoin: {
            auto request_join_op = dynamic_cast<PhysicalRequestJoinNode*>(node);
            CHECK_STATUS(GenJoin(&request_join_op->join_, node));
            break;
        }
        case kPhysicalOpRequestUnion: {
            auto request_union_op =
                dynamic_cast<PhysicalRequestUnionNode*>(node);
            CHECK_STATUS(GenRequestWindow(&request_union_op->window_,
                                          node->producers()[0]));
            for (auto& window_union :
                 request_union_op->window_unions_.window_unions_) {
                CHECK_STATUS(InitFnInfo(window_union.first, visited),
                             "Fail Gen Request Window Union Sub Query Plan");
            }
            CHECK_STATUS(
                GenRequestWindowUnionList(&request_union_op->window_unions_,
                                          request_union_op->producers()[0]));
            break;
        }
        case kPhysicalOpPostRequestUnion: {
            auto union_op = dynamic_cast<PhysicalPostRequestUnionNode*>(node);
            CHECK_STATUS(GenRange(union_op->mutable_request_ts(),
                                  node->GetProducer(0)->schemas_ctx()));
            break;
        }
        default:
            break;
    }

    // instantiate llvm functions for current node
    const auto& fn_infos = node->GetFnInfos();
    for (size_t i = 0; i < fn_infos.size(); ++i) {
        const FnInfo* fn_info = fn_infos[i];
        // skip unused fn
        if (fn_info->fn_name().empty()) {
            continue;
        }
        CHECK_STATUS(InstantiateLLVMFunction(*fn_infos[i]), "Instantiate ", i,
                     "th native function \"", fn_info->fn_name(),
                     "\" failed at node:\n", node->GetTreeString());
    }
    return Status::OK();
}

Status BatchModeTransformer::TransformLimitOp(const node::LimitPlanNode* node,
                                              PhysicalOpNode** output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError,
               "Input node or output node is null");
    PhysicalOpNode* depend = nullptr;
    CHECK_STATUS(TransformPlanOp(node->GetChildren()[0], &depend));

    PhysicalLimitNode* limit_op = nullptr;
    CHECK_STATUS(
        CreateOp<PhysicalLimitNode>(&limit_op, depend, node->limit_cnt_));

    *output = limit_op;
    return Status::OK();
}

Status BatchModeTransformer::TransformProjectPlanOp(
    const node::ProjectPlanNode* node, PhysicalOpNode** output) {
    // sanity check
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError,
               "Input node or output node is null");

    if (enable_batch_window_parallelization_) {
        return TransformProjectPlanOpWithWindowParallel(node, output);
    } else {
        return TransformProjectPlanOpWindowSerial(node, output);
    }
}
Status BatchModeTransformer::TransformProjectPlanOpWithWindowParallel(
    const node::ProjectPlanNode* node, PhysicalOpNode** output) {
    // sanity check
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError,
               "Input node or output node is null");

    PhysicalOpNode* depend = nullptr;
    if (!node->GetChildren().empty() && nullptr != node->GetChildren()[0]) {
        CHECK_STATUS(TransformPlanOp(node->GetChildren()[0], &depend));
    }
    CHECK_TRUE(!node->project_list_vec_.empty(), kPlanError,
               "Fail transform project op: empty projects");
    if (1 == node->project_list_vec_.size()) {
        return TransformProjectOp(
            dynamic_cast<hybridse::node::ProjectListNode*>(
                node->project_list_vec_[0]),
            depend, false, output);
    }

    std::vector<PhysicalOpNode*> ops;
    for (auto iter = node->project_list_vec_.cbegin();
         iter != node->project_list_vec_.cend(); iter++) {
        hybridse::node::ProjectListNode* project_list =
            dynamic_cast<hybridse::node::ProjectListNode*>(*iter);

        PhysicalOpNode* project_op = nullptr;
        CHECK_STATUS(
            TransformProjectOp(project_list, depend, false, &project_op));
        ops.push_back(project_op);
    }

    CHECK_TRUE(!ops.empty(), kPlanError,
               "Fail transform project op: empty projects");

    if (ops.size() == 1) {
        *output = ops[0];
        return Status::OK();
    }

    PhysicalJoinNode* join = nullptr;
    CHECK_STATUS(CreateOp<PhysicalJoinNode>(&join, ops[0], ops[1],
                                            ::hybridse::node::kJoinTypeConcat));

    for (size_t i = 2; i < ops.size(); ++i) {
        PhysicalJoinNode* new_join = nullptr;
        CHECK_STATUS(CreateOp<PhysicalJoinNode>(
            &new_join, join, ops[i], ::hybridse::node::kJoinTypeConcat));
        join = new_join;
    }

    auto project_list = node_manager_->MakeProjectListPlanNode(nullptr, false);
    uint32_t pos = 0;
    for (auto iter = node->pos_mapping_.cbegin();
         iter != node->pos_mapping_.cend(); iter++) {
        auto sub_project_list = dynamic_cast<node::ProjectListNode*>(
            node->project_list_vec_[iter->first]);

        auto project_node = dynamic_cast<node::ProjectNode*>(
            sub_project_list->GetProjects().at(iter->second));
        if (node::kExprAll == project_node->GetExpression()->expr_type_) {
            auto all_expr =
                dynamic_cast<node::AllNode*>(project_node->GetExpression());
            if (all_expr->children_.empty()) {
                // expand all expression if needed
                for (auto column : *depend->GetOutputSchema()) {
                    all_expr->children_.push_back(
                        node_manager_->MakeColumnRefNode(
                            column.name(), all_expr->GetRelationName()));
                }
            }
            project_list->AddProject(
                node_manager_->MakeRowProjectNode(pos, "*", all_expr));
        } else {
            project_list->AddProject(node_manager_->MakeRowProjectNode(
                pos, project_node->GetName(),
                node_manager_->MakeColumnRefNode(project_node->GetName(), "")));
        }
        pos++;
    }
    return CreatePhysicalProjectNode(kTableProject, join, project_list, false,
                                     output);
}
Status BatchModeTransformer::TransformProjectPlanOpWindowSerial(
    const node::ProjectPlanNode* node, PhysicalOpNode** output) {
    PhysicalOpNode* depend = nullptr;
    if (!node->GetChildren().empty() && nullptr != node->GetChildren()[0]) {
        CHECK_STATUS(TransformPlanOp(node->GetChildren()[0], &depend));
    }
    CHECK_TRUE(!node->project_list_vec_.empty(), kPlanError,
               "Fail transform project op: empty projects");
    if (1 == node->project_list_vec_.size()) {
        return TransformProjectOp(
            dynamic_cast<hybridse::node::ProjectListNode*>(
                node->project_list_vec_[0]),
            depend, false, output);
    }
    // 处理project_list_vec_[1...N-1], 串联执行windowAggWithAppendInput
    std::vector<PhysicalOpNode*> ops;
    int32_t project_cnt = 0;
    for (size_t i = node->project_list_vec_.size() - 1; i > 0; i--) {
        hybridse::node::ProjectListNode* project_list =
            dynamic_cast<hybridse::node::ProjectListNode*>(
                node->project_list_vec_[i]);
        project_cnt++;
        PhysicalOpNode* project_op = nullptr;
        CHECK_STATUS(
            TransformProjectOp(project_list, depend, true, &project_op));
        depend = project_op;
    }

    // 第一个Project节点除了计算投影表达式之外，还需要筛选出前面窗口的表达式结果
    // TODO(chenjing): 这部分代码可读性还是太差
    hybridse::node::ProjectListNode* first_project_list =
        dynamic_cast<hybridse::node::ProjectListNode*>(
            node->project_list_vec_[0]);
    auto project_list = node_manager_->MakeProjectListPlanNode(
        first_project_list->w_ptr_, first_project_list->has_agg_project_);
    uint32_t pos = 0;
    for (auto iter = node->pos_mapping_.cbegin();
         iter != node->pos_mapping_.cend(); iter++) {
        auto sub_project_list = dynamic_cast<node::ProjectListNode*>(
            node->project_list_vec_[iter->first]);

        auto project_node = dynamic_cast<node::ProjectNode*>(
            sub_project_list->GetProjects().at(iter->second));
        if (iter->first == 0) {
            project_list->AddProject(project_node);

        } else if (node::kExprAll ==
                   project_node->GetExpression()->expr_type_) {
            auto all_expr =
                dynamic_cast<node::AllNode*>(project_node->GetExpression());
            if (all_expr->children_.empty()) {
                // expand all expression if needed
                for (auto column : *depend->GetOutputSchema()) {
                    all_expr->children_.push_back(
                        node_manager_->MakeColumnRefNode(
                            column.name(), all_expr->GetRelationName()));
                }
            }
            project_list->AddProject(
                node_manager_->MakeRowProjectNode(pos, "*", all_expr));
        } else {
            project_list->AddProject(node_manager_->MakeRowProjectNode(
                pos, project_node->GetName(),
                node_manager_->MakeColumnRefNode(project_node->GetName(), "")));
        }
        pos++;
    }
    return TransformProjectOp(project_list, depend, false, output);
}

/**
 * Check two op outputs can union. There output schema sources
 * must exactly match on column types.
 */
static bool CheckUnionAvailable(const PhysicalOpNode* left,
                                const PhysicalOpNode* right) {
    auto left_ctx = left->schemas_ctx();
    auto right_ctx = right->schemas_ctx();
    if (left_ctx->GetSchemaSourceSize() != right_ctx->GetSchemaSourceSize()) {
        return false;
    }
    for (size_t i = 0; i < left_ctx->GetSchemaSourceSize(); ++i) {
        auto left_schema = left_ctx->GetSchema(i);
        auto right_schema = right_ctx->GetSchema(i);
        if (left_schema->size() != right_schema->size()) {
            return false;
        }
        for (auto j = 0; j < left_schema->size(); ++j) {
            if (left_schema->Get(j).type() != right_schema->Get(j).type()) {
                return false;
            }
        }
    }
    return true;
}

Status BatchModeTransformer::CreateRequestUnionNode(
    PhysicalOpNode* request, PhysicalOpNode* right,
    const std::string& primary_name, const codec::Schema* primary_schema,
    const node::ExprListNode* partition,
    const node::WindowPlanNode* window_plan,
    PhysicalRequestUnionNode** output) {
    CHECK_TRUE(request != nullptr, kPlanError, "Request node is null");
    CHECK_TRUE((partition == nullptr) ^ (window_plan == nullptr), kPlanError);

    PhysicalOpNode* left = request;
    CHECK_TRUE(right->GetOutputSchemaSourceSize() == 1, kPlanError,
               "Can not request union on right table with multiple output "
               "schema slices");

    if (!CheckUnionAvailable(left, right)) {
        // match schema between request and union table
        ColumnProjects left_projects;
        for (int i = 0; i < primary_schema->size(); ++i) {
            const std::string& col_name = primary_schema->Get(i).name();
            size_t column_id;
            CHECK_STATUS(plan_ctx_.GetRequestSourceID(primary_name, col_name,
                                                      &column_id),
                         "Fail to get request column id for ", primary_name,
                         ".", col_name);
            auto col_ref =
                node_manager_->MakeColumnRefNode(col_name, primary_name);
            left_projects.Add(col_name, col_ref, nullptr);
        }
        PhysicalSimpleProjectNode* left_project_op = nullptr;
        CHECK_STATUS(CreateOp<PhysicalSimpleProjectNode>(&left_project_op, left,
                                                         left_projects));
        left = left_project_op;
    }
    PhysicalRequestUnionNode* request_union_op = nullptr;
    if (partition != nullptr) {
        CHECK_STATUS(CreateOp<PhysicalRequestUnionNode>(&request_union_op, left,
                                                        right, partition));
    } else {
        CHECK_STATUS(CreateOp<PhysicalRequestUnionNode>(&request_union_op, left,
                                                        right, window_plan));
    }
    *output = request_union_op;
    return Status::OK();
}

Status BatchModeTransformer::TransformWindowOp(
    PhysicalOpNode* depend, const node::WindowPlanNode* w_ptr,
    PhysicalOpNode** output) {
    // sanity check
    CHECK_TRUE(depend != nullptr && output != nullptr, kPlanError,
               "Depend node or output node is null");
    CHECK_STATUS(CheckWindow(w_ptr, depend->schemas_ctx()));
    const node::OrderByNode* orders = w_ptr->GetOrders();
    const node::ExprListNode* groups = w_ptr->GetKeys();

    switch (depend->GetOpType()) {
        case kPhysicalOpRename: {
            PhysicalOpNode* new_depend;
            CHECK_STATUS(
                TransformWindowOp(depend->GetProducer(0), w_ptr, &new_depend));
            if (new_depend != depend) {
                PhysicalRenameNode* rename_op = nullptr;
                CHECK_STATUS(CreateOp<PhysicalRenameNode>(
                    &rename_op, new_depend,
                    dynamic_cast<PhysicalRenameNode*>(depend)->name_));
                *output = rename_op;
            } else {
                *output = depend;
            }
            break;
        }
        case kPhysicalOpDataProvider: {
            auto data_op = dynamic_cast<PhysicalDataProviderNode*>(depend);
            CHECK_TRUE(data_op->provider_type_ == kProviderTypeRequest,
                       kPlanError,
                       "Do not support window on non-request input");

            auto name = data_op->table_handler_->GetName();
            auto table = catalog_->GetTable(db_, name);
            CHECK_TRUE(table != nullptr, kPlanError,
                       "Fail to transform data provider op: table " + name +
                           "not exists");
            PhysicalTableProviderNode* right = nullptr;
            CHECK_STATUS(CreateOp<PhysicalTableProviderNode>(&right, table));

            PhysicalRequestUnionNode* request_union_op = nullptr;
            CHECK_STATUS(CreateRequestUnionNode(
                data_op, right, table->GetName(), table->GetSchema(), nullptr,
                w_ptr, &request_union_op));

            if (!w_ptr->union_tables().empty()) {
                for (auto iter = w_ptr->union_tables().cbegin();
                     iter != w_ptr->union_tables().cend(); iter++) {
                    PhysicalOpNode* union_table_op;
                    CHECK_STATUS(TransformPlanOp(*iter, &union_table_op));
                    CHECK_TRUE(request_union_op->AddWindowUnion(union_table_op),
                               kPlanError,
                               "Fail to add request window union table");
                }
            }
            *output = request_union_op;
            break;
        }
        case kPhysicalOpRequestJoin: {
            auto join_op = dynamic_cast<PhysicalRequestJoinNode*>(depend);
            switch (join_op->join().join_type()) {
                case node::kJoinTypeLeft:
                case node::kJoinTypeLast: {
                    auto child_schemas_ctx =
                        join_op->GetProducer(0)->schemas_ctx();
                    if (!node::ExprListNullOrEmpty(groups)) {
                        CHECK_STATUS(CheckExprDependOnChildOnly(
                                         groups, child_schemas_ctx),
                                     "Fail to handle window: group "
                                     "expression should belong to left table");
                    }
                    if (nullptr != orders &&
                        !node::ExprListNullOrEmpty(orders->order_expressions_)) {
                        CHECK_STATUS(CheckExprDependOnChildOnly(
                                         orders->order_expressions_, child_schemas_ctx),
                                     "Fail to handle window: group "
                                     "expression should belong to left table");
                    }
                    CHECK_TRUE(join_op->producers()[0]->GetOpType() ==
                                   kPhysicalOpDataProvider,
                               kPlanError,
                               "Fail to handler window with request last "
                               "join, left isn't a table provider")
                    auto request_op = dynamic_cast<PhysicalDataProviderNode*>(
                        join_op->producers()[0]);
                    auto name = request_op->table_handler_->GetName();
                    auto table = catalog_->GetTable(db_, name);
                    CHECK_TRUE(table != nullptr, kPlanError,
                               "Fail to transform data provider op: table " +
                                   name + "not exists");
                    PhysicalTableProviderNode* right = nullptr;
                    CHECK_STATUS(
                        CreateOp<PhysicalTableProviderNode>(&right, table));

                    PhysicalRequestUnionNode* request_union_op = nullptr;
                    CHECK_STATUS(CreateRequestUnionNode(
                        request_op, right, name, table->GetSchema(), nullptr,
                        w_ptr, &request_union_op));
                    if (!w_ptr->union_tables().empty()) {
                        for (auto iter = w_ptr->union_tables().cbegin();
                             iter != w_ptr->union_tables().cend(); iter++) {
                            PhysicalOpNode* union_table_op;
                            CHECK_STATUS(
                                TransformPlanOp(*iter, &union_table_op));
                            CHECK_TRUE(
                                request_union_op->AddWindowUnion(
                                    union_table_op),
                                kPlanError,
                                "Fail to add request window union table");
                        }
                    }
                    PhysicalJoinNode* join_output = nullptr;
                    CHECK_STATUS(CreateOp<PhysicalJoinNode>(
                        &join_output, request_union_op, join_op->producers()[1],
                        join_op->join_));
                    *output = join_output;
                    break;
                }
                default: {
                    return Status(kPlanError, "Non-support join type");
                }
            }
            break;
        }
        case kPhysicalOpSimpleProject: {
            auto simple_project =
                dynamic_cast<PhysicalSimpleProjectNode*>(depend);
            CHECK_TRUE(
                depend->GetProducer(0)->GetOpType() == kPhysicalOpDataProvider,
                kPlanError, "Do not support window on ",
                depend->GetTreeString());
            auto data_op =
                dynamic_cast<PhysicalDataProviderNode*>(depend->GetProducer(0));
            CHECK_TRUE(data_op->provider_type_ == kProviderTypeRequest,
                       kPlanError,
                       "Do not support window on non-request input");

            auto name = data_op->table_handler_->GetName();
            auto table = catalog_->GetTable(db_, name);
            CHECK_TRUE(table != nullptr, kPlanError,
                       "Fail to transform data provider op: table " + name +
                           "not exists");

            PhysicalTableProviderNode* right = nullptr;
            CHECK_STATUS(CreateOp<PhysicalTableProviderNode>(&right, table));

            // right side simple project
            PhysicalSimpleProjectNode* right_simple_project = nullptr;
            CHECK_STATUS(CreateOp<PhysicalSimpleProjectNode>(
                &right_simple_project, right, simple_project->project()));

            // request union
            PhysicalRequestUnionNode* request_union_op = nullptr;
            CHECK_STATUS(CreateRequestUnionNode(
                depend, right_simple_project, table->GetName(),
                table->GetSchema(), nullptr, w_ptr, &request_union_op));
            if (!w_ptr->union_tables().empty()) {
                for (auto iter = w_ptr->union_tables().cbegin();
                     iter != w_ptr->union_tables().cend(); iter++) {
                    PhysicalOpNode* union_table_op;
                    CHECK_STATUS(TransformPlanOp(*iter, &union_table_op));
                    CHECK_TRUE(request_union_op->AddWindowUnion(union_table_op),
                               kPlanError,
                               "Fail to add request window union table");
                }
            }
            *output = request_union_op;
            break;
        }
        default: {
            return Status(kPlanError, "Do not support window on " +
                                          depend->GetTreeString());
        }
    }
    return Status::OK();
}

Status BatchModeTransformer::TransformJoinOp(const node::JoinPlanNode* node,
                                             PhysicalOpNode** output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError,
               "Input node or output node is null");

    PhysicalOpNode* left = nullptr;
    CHECK_STATUS(TransformPlanOp(node->GetChildren()[0], &left));
    PhysicalOpNode* right = nullptr;
    CHECK_STATUS(TransformPlanOp(node->GetChildren()[1], &right));

    PhysicalJoinNode* join_op = nullptr;
    CHECK_STATUS(CreateOp<PhysicalJoinNode>(&join_op, left, right,
                                            node->join_type_, node->orders_,
                                            node->condition_));
    CHECK_STATUS(
        CheckTimeOrIntegerOrderColumn(node->orders_, join_op->schemas_ctx()));
    *output = join_op;
    return Status::OK();
}

Status BatchModeTransformer::TransformUnionOp(const node::UnionPlanNode* node,
                                              PhysicalOpNode** output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError,
               "Input node or output node is null");

    PhysicalOpNode* left = nullptr;
    CHECK_STATUS(TransformPlanOp(node->GetChildren()[0], &left));
    PhysicalOpNode* right = nullptr;
    CHECK_STATUS(TransformPlanOp(node->GetChildren()[1], &right));

    CHECK_TRUE(CheckUnionAvailable(left, right), kPlanError,
               "Union inputs can not take inconsistent schema");
    PhysicalUnionNode* union_op = nullptr;
    CHECK_STATUS(
        CreateOp<PhysicalUnionNode>(&union_op, left, right, node->is_all));
    *output = union_op;
    return Status::OK();
}

Status BatchModeTransformer::TransformGroupOp(const node::GroupPlanNode* node,
                                              PhysicalOpNode** output) {
    PhysicalOpNode* left = nullptr;
    CHECK_STATUS(TransformPlanOp(node->GetChildren()[0], &left));

    if (kPhysicalOpDataProvider == left->GetOpType()) {
        auto data_op = dynamic_cast<PhysicalDataProviderNode*>(left);
        if (kProviderTypeRequest == data_op->provider_type_) {
            auto name = data_op->table_handler_->GetName();
            auto table = catalog_->GetTable(db_, name);
            CHECK_TRUE(table != nullptr, kPlanError,
                       "Fail to transform data provider op: table " + name +
                           "not exists");

            PhysicalTableProviderNode* right = nullptr;
            CHECK_STATUS(CreateOp<PhysicalTableProviderNode>(&right, table));

            PhysicalRequestUnionNode* request_union_op = nullptr;
            CHECK_STATUS(CreateRequestUnionNode(
                data_op, right, table->GetName(), table->GetSchema(),
                node->by_list_, nullptr, &request_union_op));
            *output = request_union_op;
            return Status::OK();
        }
    }
    PhysicalGroupNode* group_op = nullptr;
    CHECK_STATUS(CreateOp<PhysicalGroupNode>(&group_op, left, node->by_list_));
    *output = group_op;
    return Status::OK();
}

Status BatchModeTransformer::TransformSortOp(const node::SortPlanNode* node,
                                             PhysicalOpNode** output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError,
               "Input node or output node is null");
    PhysicalOpNode* left = nullptr;
    CHECK_STATUS(TransformPlanOp(node->GetChildren()[0], &left));
    if (nullptr == node->order_by_ || node::ExprListNullOrEmpty(node->order_by_->order_expressions_)) {
        DLOG(INFO) << "Skip transform sort op when order is null or empty";
        *output = left;
        return Status::OK();
    }
    CHECK_STATUS(CheckTimeOrIntegerOrderColumn(node->order_by_, left->schemas_ctx()))
    PhysicalSortNode* sort_op = nullptr;
    CHECK_STATUS(CreateOp<PhysicalSortNode>(&sort_op, left, node->order_by_));
    *output = sort_op;
    return Status::OK();
}

Status BatchModeTransformer::TransformFilterOp(const node::FilterPlanNode* node,
                                               PhysicalOpNode** output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError,
               "Input node or output node is null");
    PhysicalOpNode* depend = nullptr;
    CHECK_STATUS(TransformPlanOp(node->GetChildren()[0], &depend));

    PhysicalFilterNode* filter_op = nullptr;
    CHECK_STATUS(
        CreateOp<PhysicalFilterNode>(&filter_op, depend, node->condition_));
    *output = filter_op;
    return Status::OK();
}

Status BatchModeTransformer::TransformScanOp(const node::TablePlanNode* node,
                                             PhysicalOpNode** output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError,
               "Input node or output node is null");
    auto table = catalog_->GetTable(db_, node->table_);
    CHECK_TRUE(table != nullptr, kPlanError,
               "Fail to transform data provider op: table " + node->table_ +
                   "not exists");

    PhysicalTableProviderNode* table_op = nullptr;
    CHECK_STATUS(CreateOp<PhysicalTableProviderNode>(&table_op, table));
    *output = table_op;
    return Status::OK();
}

Status BatchModeTransformer::TransformRenameOp(const node::RenamePlanNode* node,
                                               PhysicalOpNode** output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError,
               "Input node or output node is null");
    PhysicalOpNode* depend = nullptr;
    CHECK_STATUS(TransformPlanOp(node->GetChildren()[0], &depend));

    PhysicalRenameNode* rename_op = nullptr;
    CHECK_STATUS(
        CreateOp<PhysicalRenameNode>(&rename_op, depend, node->table_));
    *output = rename_op;
    return Status::OK();
}

Status BatchModeTransformer::TransformDistinctOp(
    const node::DistinctPlanNode* node, PhysicalOpNode** output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError,
               "Input node or output node is null");
    PhysicalOpNode* depend = nullptr;
    CHECK_STATUS(TransformPlanOp(node->GetChildren()[0], &depend));

    PhysicalDistinctNode* distinct_op = nullptr;
    CHECK_STATUS(CreateOp<PhysicalDistinctNode>(&distinct_op, depend));
    *output = distinct_op;
    return Status::OK();
}

Status BatchModeTransformer::TransformQueryPlan(
    const ::hybridse::node::PlanNode* node,
    ::hybridse::vm::PhysicalOpNode** output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError,
               "Input node or output node is null");
    return TransformPlanOp(node->GetChildren()[0], output);
}

bool BatchModeTransformer::AddPass(PhysicalPlanPassType type) {
    passes.push_back(type);
    return true;
}

Status ExtractProjectInfos(const node::PlanNodeList& projects,
                           const node::FrameNode* primary_frame,
                           const SchemasContext* schemas_ctx,
                           node::NodeManager* node_manager,
                           ColumnProjects* output) {
    for (auto plan_node : projects) {
        auto pp_node = dynamic_cast<node::ProjectNode*>(plan_node);
        CHECK_TRUE(pp_node != nullptr, kPlanError);
        auto expr = pp_node->GetExpression();
        CHECK_TRUE(expr != nullptr, kPlanError);
        if (expr->GetExprType() == node::kExprAll) {
            // expand *
            for (size_t slice = 0; slice < schemas_ctx->GetSchemaSourceSize();
                 ++slice) {
                auto schema_source = schemas_ctx->GetSchemaSource(slice);
                for (size_t k = 0; k < schema_source->size(); ++k) {
                    auto col_name = schema_source->GetColumnName(k);
                    auto col_ref = node_manager->MakeColumnRefNode(
                        col_name, schema_source->GetSourceName());
                    output->Add(col_name, col_ref, nullptr);
                }
            }
        } else {
            output->Add(pp_node->GetName(), expr, pp_node->frame());
        }
    }
    output->SetPrimaryFrame(primary_frame);
    return Status::OK();
}

Status BatchModeTransformer::InstantiateLLVMFunction(const FnInfo& fn_info) {
    CHECK_TRUE(fn_info.IsValid(), kCodegenError, "Fail to install llvm function, function info is invalid");
    codegen::CodeGenContext codegen_ctx(module_, fn_info.schemas_ctx(), plan_ctx_.parameter_types(), node_manager_);
    codegen::RowFnLetIRBuilder builder(&codegen_ctx);
    return builder.Build(fn_info.fn_name(), fn_info.fn_def(), fn_info.GetPrimaryFrame(), fn_info.GetFrames(),
                         *fn_info.fn_schema());
}

bool BatchModeTransformer::AddDefaultPasses() {
    AddPass(PhysicalPlanPassType::kPassColumnProjectsOptimized);
    AddPass(PhysicalPlanPassType::kPassFilterOptimized);
    AddPass(PhysicalPlanPassType::kPassLeftJoinOptimized);
    AddPass(PhysicalPlanPassType::kPassGroupAndSortOptimized);
    AddPass(PhysicalPlanPassType::kPassLimitOptimized);
    AddPass(PhysicalPlanPassType::kPassClusterOptimized);
    return false;
}

Status BatchModeTransformer::CreatePhysicalConstProjectNode(
    node::ProjectListNode* project_list, PhysicalOpNode** output) {
    CHECK_TRUE(project_list != nullptr && output != nullptr, kPlanError,
               "Project list node or output node is null");

    const node::PlanNodeList& projects = project_list->GetProjects();
    for (auto iter = projects.cbegin(); iter != projects.cend(); iter++) {
        auto project_node = dynamic_cast<node::ProjectNode*>(*iter);
        auto expr = project_node->GetExpression();
        CHECK_TRUE(expr != nullptr, kPlanError,
                   "Invalid project: expression is null");
        CHECK_TRUE(node::kExprAll != expr->expr_type_, kPlanError,
                   "Invalid project: no table used");
    }

    SchemasContext empty_schemas_ctx;
    ColumnProjects const_projects;
    CHECK_STATUS(ExtractProjectInfos(projects, nullptr, &empty_schemas_ctx,
                                     node_manager_, &const_projects));

    PhysicalConstProjectNode* const_project_op = nullptr;
    CHECK_STATUS(
        CreateOp<PhysicalConstProjectNode>(&const_project_op, const_projects));
    *output = const_project_op;
    return Status::OK();
}

Status BatchModeTransformer::CreatePhysicalProjectNode(
    const ProjectType project_type, PhysicalOpNode* depend,
    node::ProjectListNode* project_list, bool append_input,
    PhysicalOpNode** output) {
    // sanity checks
    CHECK_TRUE(project_list != nullptr && output != nullptr, kPlanError,
               "project list node or output node is null");

    const node::PlanNodeList& projects = project_list->GetProjects();

    // extract window frame if any
    const node::FrameNode* primary_frame = nullptr;
    if (project_list->GetW() != nullptr) {
        primary_frame = project_list->GetW()->frame_node();
    }

    node::PlanNodeList new_projects;
    bool has_all_project = false;

    for (auto iter = projects.cbegin(); iter != projects.cend(); iter++) {
        auto project_node = dynamic_cast<node::ProjectNode*>(*iter);
        auto expr = project_node->GetExpression();
        CHECK_TRUE(expr != nullptr, kPlanError,
                   "Invalid project: expression is null");
        if (node::kExprAll == expr->expr_type_) {
            auto all_expr = dynamic_cast<node::AllNode*>(expr);
            if (all_expr->children_.empty()) {
                // expand all expression if needed
                for (auto column : *depend->GetOutputSchema()) {
                    all_expr->children_.push_back(
                        node_manager_->MakeColumnRefNode(
                            column.name(), all_expr->GetRelationName()));
                }
            }
            has_all_project = true;
        }
    }

    if (has_all_project && 1 == projects.size() &&
        depend->GetOutputSchemaSourceSize() == 1) {
        // skip project
        DLOG(INFO) << "skip project node: project has only kAllExpr "
                      "expression";
        *output = depend;
        return Status::OK();
    }

    // Create project function and infer output schema
    ColumnProjects column_projects;
    CHECK_STATUS(ExtractProjectInfos(projects, primary_frame,
                                     depend->schemas_ctx(), node_manager_,
                                     &column_projects));

    if (append_input) {
        CHECK_TRUE(project_type == kWindowAggregation, kPlanError,
                   "Only window agg allow append input");
    }

    // Create physical project op
    switch (project_type) {
        case kRowProject: {
            if (IsSimpleProject(column_projects)) {
                PhysicalSimpleProjectNode* simple_project_op = nullptr;
                CHECK_STATUS(CreateOp<PhysicalSimpleProjectNode>(
                    &simple_project_op, depend, column_projects));
                *output = simple_project_op;
            } else {
                PhysicalRowProjectNode* row_project_op = nullptr;
                CHECK_STATUS(CreateOp<PhysicalRowProjectNode>(
                    &row_project_op, depend, column_projects));
                *output = row_project_op;
            }
            break;
        }
        case kTableProject: {
            if (IsSimpleProject(column_projects)) {
                PhysicalSimpleProjectNode* simple_project_op = nullptr;
                CHECK_STATUS(CreateOp<PhysicalSimpleProjectNode>(
                    &simple_project_op, depend, column_projects));
                *output = simple_project_op;
            } else {
                PhysicalTableProjectNode* table_project_op = nullptr;
                CHECK_STATUS(CreateOp<PhysicalTableProjectNode>(
                    &table_project_op, depend, column_projects));
                *output = table_project_op;
            }
            break;
        }
        case kAggregation: {
            PhysicalAggrerationNode* agg_op = nullptr;
            CHECK_STATUS(CreateOp<PhysicalAggrerationNode>(&agg_op, depend,
                                                           column_projects));
            *output = agg_op;
            break;
        }
        case kGroupAggregation: {
            const node::ExprListNode* keys = nullptr;
            CHECK_STATUS(ExtractGroupKeys(depend, &keys));
            CHECK_TRUE(keys != nullptr, kPlanError,
                       "Can not create group agg with non group keys");
            PhysicalGroupAggrerationNode* agg_op = nullptr;
            CHECK_STATUS(CreateOp<PhysicalGroupAggrerationNode>(
                &agg_op, depend, column_projects, keys));
            *output = agg_op;
            break;
        }
        case kWindowAggregation: {
            PhysicalWindowAggrerationNode* window_agg_op = nullptr;
            CHECK_STATUS(CreateOp<PhysicalWindowAggrerationNode>(
                &window_agg_op, depend, column_projects,
                WindowOp(project_list->w_ptr_),
                project_list->w_ptr_->instance_not_in_window(), append_input,
                project_list->w_ptr_->exclude_current_time()));
            if (!project_list->w_ptr_->union_tables().empty()) {
                for (auto iter = project_list->w_ptr_->union_tables().cbegin();
                     iter != project_list->w_ptr_->union_tables().cend();
                     iter++) {
                    PhysicalOpNode* union_table_op;
                    CHECK_STATUS(TransformPlanOp(*iter, &union_table_op));
                    CHECK_TRUE(window_agg_op->AddWindowUnion(union_table_op),
                               kPlanError, "Fail to add window union table");
                }
            }
            *output = window_agg_op;
            break;
        }
        default:
            return Status(kPlanError, "Unknown project type");
    }
    return Status::OK();
}

base::Status BatchModeTransformer::ExtractGroupKeys(vm::PhysicalOpNode* depend, const node::ExprListNode** keys) {
    CHECK_TRUE(nullptr != depend, common::kNullPointer, "Invalid op, is null")
    if (depend->GetOpType() == kPhysicalOpFilter) {
        CHECK_STATUS(ExtractGroupKeys(depend->GetProducer(0), keys))
        return base::Status::OK();
    }
    CHECK_TRUE(depend->GetOpType() == kPhysicalOpGroupBy, kPlanError, "Fail to extract group keys from op ",
               vm::PhysicalOpTypeName(depend->GetOpType()))
    *keys = dynamic_cast<PhysicalGroupNode*>(depend)->group().keys_;
    return base::Status::OK();
}
Status BatchModeTransformer::TransformProjectOp(
    node::ProjectListNode* project_list, PhysicalOpNode* node,
    bool append_input, PhysicalOpNode** output) {
    auto depend = node;
    if (nullptr == depend) {
        return CreatePhysicalConstProjectNode(project_list, output);
    }
    switch (depend->GetOutputType()) {
        case kSchemaTypeRow:
            return CreatePhysicalProjectNode(kRowProject, depend, project_list,
                                             append_input, output);
        case kSchemaTypeGroup:
            return CreatePhysicalProjectNode(
                kGroupAggregation, depend, project_list, append_input, output);
        case kSchemaTypeTable:
            if (project_list->HasAggProject()) {
                if (project_list->IsWindowProject()) {
                    CHECK_STATUS(
                        CheckWindow(project_list->w_ptr_, depend->schemas_ctx()));
                    return CreatePhysicalProjectNode(kWindowAggregation, depend,
                                                     project_list, append_input,
                                                     output);
                } else {
                    return CreatePhysicalProjectNode(kAggregation, depend,
                                                     project_list, append_input,
                                                     output);
                }

            } else {
                return CreatePhysicalProjectNode(
                    kTableProject, depend, project_list, append_input, output);
            }
        default:
            return Status(kPlanError, "Unknown node output type");
    }
}

void BatchModeTransformer::ApplyPasses(PhysicalOpNode* node,
                                       PhysicalOpNode** output) {
    // do not change physical plan if pass failed
    *output = node;
    PhysicalOpNode* cur_op = node;
    for (auto type : passes) {
        bool transformed = false;
        PhysicalOpNode* new_op = nullptr;
        switch (type) {
            case PhysicalPlanPassType::kPassColumnProjectsOptimized: {
                SimpleProjectOptimized pass(&plan_ctx_);
                transformed = pass.Apply(cur_op, &new_op);
                break;
            }
            case PhysicalPlanPassType::kPassFilterOptimized: {
                ConditionOptimized pass(&plan_ctx_);
                transformed = pass.Apply(cur_op, &new_op);
                break;
            }
            case PhysicalPlanPassType::kPassGroupAndSortOptimized: {
                GroupAndSortOptimized pass(&plan_ctx_);
                transformed = pass.Apply(cur_op, &new_op);
                break;
            }
            case PhysicalPlanPassType::kPassLeftJoinOptimized: {
                if (catalog_->IndexSupport()) {
                    LeftJoinOptimized pass(&plan_ctx_);
                    transformed = pass.Apply(cur_op, &new_op);
                }
                break;
            }
            case PhysicalPlanPassType::kPassClusterOptimized: {
                if (cluster_optimized_mode_) {
                    ClusterOptimized pass(&plan_ctx_);
                    transformed = pass.Apply(cur_op, &new_op);
                }
                break;
            }
            case PhysicalPlanPassType::kPassLimitOptimized: {
                LimitOptimized pass(&plan_ctx_);
                transformed = pass.Apply(cur_op, &new_op);
                break;
            }
            default: {
                DLOG(WARNING) << "Invalid pass: "
                             << PhysicalPlanPassTypeName(type);
            }
        }
        if (transformed && new_op != nullptr) {
            cur_op = new_op;
        }
    }
    if (cur_op != nullptr) {
        *output = cur_op;
    } else {
        DLOG(WARNING) << "Final transformed result is null";
    }

    if (enable_batch_window_parallelization_) {
        DLOG(INFO) << "Apply column pruning for window aggregation";
        WindowColumnPruning pass;
        PhysicalOpNode* pruned_op = nullptr;
        Status status = pass.Apply(&plan_ctx_, *output, &pruned_op);
        if (!status.isOK()) {
            DLOG(WARNING) << status;
            return;
        }
        *output = pruned_op;
    }
}

std::string BatchModeTransformer::ExtractSchemaName(PhysicalOpNode* in) {
    if (nullptr == in) {
        return "";
    }
    if (kPhysicalOpSimpleProject == in->GetOpType()) {
        return ExtractSchemaName(in->GetProducer(0));
    } else if (kPhysicalOpRename == in->GetOpType()) {
        return dynamic_cast<PhysicalRenameNode*>(in)->name_;
    } else if (kPhysicalOpDataProvider == in->GetOpType()) {
        auto data_provider = dynamic_cast<PhysicalDataProviderNode*>(in);
        return data_provider->table_handler_->GetName();
    } else {
        return "";
    }
    return "";
}
bool BatchModeTransformer::isSourceFromTableOrPartition(PhysicalOpNode* in) {
    if (nullptr == in) {
        DLOG(WARNING) << "Invalid physical node: null";
        return false;
    }
    if (kPhysicalOpSimpleProject == in->GetOpType() ||
        kPhysicalOpRename == in->GetOpType()) {
        return isSourceFromTableOrPartition(in->GetProducer(0));
    } else if (kPhysicalOpDataProvider == in->GetOpType()) {
        return kProviderTypePartition ==
                   dynamic_cast<PhysicalDataProviderNode*>(in)
                       ->provider_type_ ||
               kProviderTypeTable ==
                   dynamic_cast<PhysicalDataProviderNode*>(in)->provider_type_;
    }
    return false;
}
bool BatchModeTransformer::isSourceFromTable(PhysicalOpNode* in) {
    if (nullptr == in) {
        DLOG(WARNING) << "Invalid physical node: null";
        return false;
    }
    if (kPhysicalOpSimpleProject == in->GetOpType() ||
        kPhysicalOpRename == in->GetOpType()) {
        return isSourceFromTableOrPartition(in->GetProducer(0));
    } else if (kPhysicalOpDataProvider == in->GetOpType()) {
        return kProviderTypeTable ==
               dynamic_cast<PhysicalDataProviderNode*>(in)->provider_type_;
    }
    return false;
}
Status BatchModeTransformer::ValidateTableProvider(PhysicalOpNode* in) {
    CHECK_TRUE(nullptr != in, kPlanError, "Invalid physical node: null");
    CHECK_TRUE(isSourceFromTableOrPartition(in), kPlanError,
               "Isn't table/partition provider")
    return Status::OK();
}
Status BatchModeTransformer::ValidateRequestDataProvider(PhysicalOpNode* in) {
    CHECK_TRUE(nullptr != in, kPlanError, "Invalid physical node: null");
    if (kPhysicalOpSimpleProject == in->GetOpType() ||
        kPhysicalOpRename == in->GetOpType()) {
        CHECK_STATUS(ValidateRequestDataProvider(in->GetProducer(0)))
    } else {
        CHECK_TRUE(
            kPhysicalOpDataProvider == in->GetOpType() &&
                kProviderTypeRequest ==
                    dynamic_cast<PhysicalDataProviderNode*>(in)->provider_type_,
            kPlanError, "Isn't partition provider");
    }
    return Status::OK();
}
Status BatchModeTransformer::ValidatePartitionDataProvider(PhysicalOpNode* in) {
    CHECK_TRUE(nullptr != in, kPlanError, "Invalid physical node: null");
    if (kPhysicalOpSimpleProject == in->GetOpType() ||
        kPhysicalOpRename == in->GetOpType()) {
        CHECK_STATUS(ValidatePartitionDataProvider(in->GetProducer(0)))
    } else {
        CHECK_TRUE(
            kPhysicalOpDataProvider == in->GetOpType() &&
                kProviderTypePartition ==
                    dynamic_cast<PhysicalDataProviderNode*>(in)->provider_type_,
            kPlanError, "Isn't partition provider");
    }
    return Status::OK();
}

Status BatchModeTransformer::ValidateJoinIndexOptimization(
    const Join& join, PhysicalOpNode* right) {
    CHECK_TRUE(nullptr != right, kPlanError, "Invalid physical node: null");
    if (node::kJoinTypeConcat == join.join_type_) {
        return Status::OK();
    }

    if (node::kJoinTypeLast == join.join_type_) {
        CHECK_TRUE(
            nullptr == join.right_sort_.orders() ||
                node::ExprListNullOrEmpty(join.right_sort_.orders()->order_expressions_)
                || nullptr == join.right_sort().orders()->GetOrderExpressionExpr(0),
            kPlanError, "Last Join node order by hasn't been optimized")
    }
    if (kSchemaTypeRow == right->GetOutputType()) {
        // skip index optimization check when join a row
        return Status::OK();
    } else {
        CHECK_STATUS(ValidatePartitionDataProvider(right),
                     "Join node hasn't been optimized");
    }
    return Status::OK();
}

Status BatchModeTransformer::ValidateWindowIndexOptimization(
    const WindowOp& window, PhysicalOpNode* in) {
    CHECK_TRUE(nullptr != in, kPlanError, "Invalid physical node: null");
    CHECK_STATUS(ValidatePartitionDataProvider(in),
                 "Window node hasn't been optimized");
    // check if window's order expression has been optimized
    CHECK_TRUE(!window.sort().ValidSort() ||
                   node::ExprListNullOrEmpty(window.sort().orders()->order_expressions_)
                   || nullptr == window.sort().orders()->GetOrderExpressionExpr(0),
               kPlanError, "Window node hasn't been optimzied")
    return Status::OK();
}
Status BatchModeTransformer::ValidatePlan(PhysicalOpNode* node) {
    if (performance_sensitive_mode_) {
        CHECK_STATUS(ValidateIndexOptimization(node),
                     "Fail to support physical plan in "
                     "performance sensitive mode");
        CHECK_STATUS(ValidateNotAggregationOverTable(node),
                     "Fail to support aggregation over table in performance sensitive mode")
    }
    return Status::OK();
}

// Validate plan in request mode
// Request mode should validate primary path and primary source
Status RequestModeTransformer::ValidatePlan(PhysicalOpNode* node) {
    CHECK_STATUS(BatchModeTransformer::ValidatePlan(node))
    PhysicalOpNode* primary_source = nullptr;
    CHECK_STATUS(ValidatePrimaryPath(node, &primary_source),
                 "Fail to validate physical plan")
    return Status::OK();
}
Status BatchModeTransformer::ValidateNotAggregationOverTable(PhysicalOpNode* in) {
    CHECK_TRUE(nullptr != in, kPlanError, "Null physical node")
    switch (in->GetOpType()) {
        case kPhysicalOpProject: {
            auto project_op = dynamic_cast<PhysicalProjectNode*>(in);
            CHECK_TRUE(in->GetProducerCnt() > 0, kPlanError, "Invalid Project Op with no producers")
            if (kAggregation == project_op->project_type_) {
               CHECK_TRUE(!isSourceFromTable(in->GetProducer(0)), kPlanError,
                           "Aggregation over a table source")
            }
            break;
        }
        default: {
            break;
        }
    }
    for (uint32_t i = 0; i < in->GetProducerCnt(); i++) {
        CHECK_STATUS(ValidateNotAggregationOverTable(in->GetProducer(i)));
    }
    return Status::OK();
}
Status BatchModeTransformer::ValidateIndexOptimization(PhysicalOpNode* in) {
    CHECK_TRUE(nullptr != in, kPlanError, "Invalid physical node: null");

    switch (in->GetOpType()) {
        case kPhysicalOpGroupBy: {
            CHECK_STATUS(ValidatePartitionDataProvider(in->GetProducer(0)));
            break;
        }
        case kPhysicalOpProject: {
            auto project_op = dynamic_cast<PhysicalProjectNode*>(in);
            if (kWindowAggregation == project_op->project_type_) {
                PhysicalWindowAggrerationNode* union_op =
                    dynamic_cast<PhysicalWindowAggrerationNode*>(project_op);
                if (!union_op->instance_not_in_window()) {
                    CHECK_STATUS(ValidateWindowIndexOptimization(
                        union_op->window(), union_op->GetProducer(0)));
                }
                if (!union_op->window_joins().Empty()) {
                    for (auto& window_join :
                         union_op->window_joins().window_joins()) {
                        CHECK_STATUS(ValidateJoinIndexOptimization(
                            window_join.second, window_join.first));
                    }
                }
                if (!union_op->window_unions().Empty()) {
                    for (auto& window_union :
                         union_op->window_unions().window_unions_) {
                        CHECK_STATUS(ValidateWindowIndexOptimization(
                            window_union.second, window_union.first));
                    }
                }
            }
            break;
        }
        case kPhysicalOpRequestUnion: {
            PhysicalRequestUnionNode* union_op =
                dynamic_cast<PhysicalRequestUnionNode*>(in);
            if (!union_op->instance_not_in_window()) {
                CHECK_STATUS(ValidateWindowIndexOptimization(
                    union_op->window(), union_op->GetProducer(1)));
            }
            if (!union_op->window_unions().Empty()) {
                for (auto& window_union :
                     union_op->window_unions().window_unions_) {
                    CHECK_STATUS(ValidateWindowIndexOptimization(
                        window_union.second, window_union.first));
                }
            }
            break;
        }
        case kPhysicalOpRequestJoin: {
            PhysicalRequestJoinNode* join_op =
                dynamic_cast<PhysicalRequestJoinNode*>(in);
            CHECK_TRUE(nullptr != in, kPlanError, "Invalid join node: null");
            CHECK_STATUS(ValidateJoinIndexOptimization(
                join_op->join(), join_op->GetProducer(1)));
            break;
        }
        case kPhysicalOpJoin: {
            PhysicalJoinNode* join_op = dynamic_cast<PhysicalJoinNode*>(in);
            CHECK_TRUE(nullptr != in, kPlanError, "Invalid join node: null");
            CHECK_STATUS(ValidateJoinIndexOptimization(
                join_op->join(), join_op->GetProducer(1)));
            break;
        }
        case kPhysicalOpFilter: {
            PhysicalFilterNode* filter_op =
                dynamic_cast<PhysicalFilterNode*>(in);
            CHECK_TRUE(nullptr != in, kPlanError, "Invalid filter node: null")
            CHECK_STATUS(
                ValidatePartitionDataProvider(filter_op->GetProducer(0)));
            break;
        }
        default: {
            break;
        }
    }

    for (uint32_t i = 0; i < in->GetProducerCnt(); i++) {
        CHECK_STATUS(ValidateIndexOptimization(in->GetProducer(i)));
    }
    return Status::OK();
}

Status BatchModeTransformer::TransformPhysicalPlan(
    const ::hybridse::node::PlanNodeList& trees,
    ::hybridse::vm::PhysicalOpNode** output) {
    CHECK_TRUE(module_ != nullptr && !trees.empty(), kPlanError,
               "Module or logical trees is empty");

    auto it = trees.begin();
    for (; it != trees.end(); ++it) {
        const ::hybridse::node::PlanNode* node = *it;
        switch (node->GetType()) {
            case ::hybridse::node::kPlanTypeFuncDef: {
                const ::hybridse::node::FuncDefPlanNode* func_def_plan =
                    dynamic_cast<const ::hybridse::node::FuncDefPlanNode*>(
                        node);
                CHECK_STATUS(GenFnDef(func_def_plan),
                             "Fail to compile user function def");
                *output = nullptr;
                break;
            }
            case ::hybridse::node::kPlanTypeUnion:
            case ::hybridse::node::kPlanTypeQuery: {
                PhysicalOpNode* physical_plan = nullptr;
                CHECK_STATUS(TransformQueryPlan(node, &physical_plan),
                             "Fail to transform query statement");

                DLOG(INFO) << "Before optimization: \n"
                           << physical_plan->GetTreeString();

                PhysicalOpNode* optimized_physical_plan = nullptr;
                ApplyPasses(physical_plan, &optimized_physical_plan);

                DLOG(INFO) << "After optimization: \n"
                           << optimized_physical_plan->GetTreeString();
                CHECK_STATUS(ValidatePlan(optimized_physical_plan));
                std::set<PhysicalOpNode*> node_visited_dict;
                CHECK_STATUS(
                    InitFnInfo(optimized_physical_plan, &node_visited_dict),
                    "Fail to generate functions for physical plan");
                *output = optimized_physical_plan;
                break;
            }
            case ::hybridse::node::kPlanTypeCreateSp: {
                const ::hybridse::node::CreateProcedurePlanNode* sp_plan =
                    dynamic_cast<
                        const ::hybridse::node::CreateProcedurePlanNode*>(node);
                return TransformPhysicalPlan(sp_plan->GetInnerPlanNodeList(),
                                             output);
            }
            default: {
                return Status(kPlanError,
                              "Plan type not supported: " +
                                  node::NameOfPlanNodeType(node->GetType()));
            }
        }
    }
    return Status::OK();
}

Status BatchModeTransformer::GenFnDef(const node::FuncDefPlanNode* fn_plan) {
    CHECK_TRUE(
        module_ != nullptr && fn_plan != nullptr && fn_plan->fn_def_ != nullptr,
        kPlanError, "Fail to codegen function: module or fn_def node is null");

    ::hybridse::codegen::FnIRBuilder builder(module_);
    ::llvm::Function* fn = nullptr;
    Status status;
    bool ok = builder.Build(fn_plan->fn_def_, &fn, status);
    CHECK_STATUS(status)

    type::Type column_type;
    auto header = fn_plan->fn_def_->header_;
    CHECK_TRUE(codegen::DataType2SchemaType(*header->ret_type_, &column_type),
               kPlanError, "UDF return type error");
    plan_ctx_.legacy_udf_dict_[header->name_] = column_type;

    return status;
}

Status BatchModeTransformer::GenJoin(Join* join, PhysicalOpNode* in) {
    const SchemasContext* joined_ctx = nullptr;
    if (in->GetOpType() == kPhysicalOpJoin) {
        joined_ctx = dynamic_cast<PhysicalJoinNode*>(in)->joined_schemas_ctx();
    } else if (in->GetOpType() == kPhysicalOpRequestJoin) {
        joined_ctx =
            dynamic_cast<PhysicalRequestJoinNode*>(in)->joined_schemas_ctx();
    }
    CHECK_TRUE(joined_ctx != nullptr, kPlanError);
    CHECK_STATUS(GenConditionFilter(&join->condition_, joined_ctx));
    CHECK_STATUS(GenKey(&join->left_key_, in->producers()[0]->schemas_ctx()));
    CHECK_STATUS(GenKey(&join->index_key_, in->producers()[0]->schemas_ctx()));
    CHECK_STATUS(GenKey(&join->right_key_, in->producers()[1]->schemas_ctx()));
    if (join->join_type_ == node::kJoinTypeLast) {
        CHECK_STATUS(
            GenSort(&join->right_sort_, in->producers()[1]->schemas_ctx()));
    }
    return Status::OK();
}

Status BatchModeTransformer::GenFilter(Filter* filter, PhysicalOpNode* in) {
    CHECK_STATUS(GenConditionFilter(&filter->condition_, in->schemas_ctx()));
    CHECK_STATUS(GenKey(&filter->left_key_, in->schemas_ctx()));
    CHECK_STATUS(GenKey(&filter->index_key_, in->schemas_ctx()));
    CHECK_STATUS(GenKey(&filter->right_key_, in->schemas_ctx()));
    return Status::OK();
}

Status BatchModeTransformer::GenConditionFilter(
    ConditionFilter* filter, const SchemasContext* schemas_ctx) {
    if (nullptr != filter->condition_) {
        node::ExprListNode expr_list;
        expr_list.AddChild(const_cast<node::ExprNode*>(filter->condition_));
        CHECK_STATUS(plan_ctx_.InitFnDef(&expr_list, schemas_ctx, true, filter))
    }
    return Status::OK();
}

Status BatchModeTransformer::GenKey(Key* hash,
                                    const SchemasContext* schemas_ctx) {
    if (hash != nullptr && !node::ExprListNullOrEmpty(hash->keys())) {
        CHECK_STATUS(
            plan_ctx_.InitFnDef(hash->keys(), schemas_ctx, true, hash));
    }
    return Status::OK();
}

Status BatchModeTransformer::GenWindow(WindowOp* window, PhysicalOpNode* in) {
    CHECK_STATUS(GenKey(&window->partition_, in->schemas_ctx()));
    CHECK_STATUS(GenSort(&window->sort_, in->schemas_ctx()));
    CHECK_STATUS(GenRange(&window->range_, in->schemas_ctx()));
    return Status::OK();
}

Status BatchModeTransformer::GenRequestWindow(RequestWindowOp* window,
                                              PhysicalOpNode* in) {
    CHECK_STATUS(GenWindow(window, in));
    CHECK_STATUS(GenKey(&window->index_key_, in->schemas_ctx()));
    return Status::OK();
}

Status BatchModeTransformer::GenSort(Sort* sort, const SchemasContext* schemas_ctx) {
    if (nullptr != sort->orders_ && !node::ExprListNullOrEmpty(sort->orders()->order_expressions())) {
        node::ExprListNode exprs;
        for (int i = 0; i < sort->orders_->order_expressions_->GetChildNum(); i++) {
            auto expr = sort->orders_->GetOrderExpressionExpr(i);
            if (nullptr != expr) {
                exprs.AddChild(const_cast<node::ExprNode*>(expr));
            }
        }
        if (!node::ExprListNullOrEmpty(&exprs)) {
            CHECK_STATUS(plan_ctx_.InitFnDef(&exprs, schemas_ctx, true, sort))
        }
    }
    return Status::OK();
}

Status BatchModeTransformer::GenRange(Range* range,
                                      const SchemasContext* schemas_ctx) {
    if (nullptr != range->range_key_) {
        node::ExprListNode expr_list;
        expr_list.AddChild(const_cast<node::ExprNode*>(range->range_key_));
        CHECK_STATUS(plan_ctx_.InitFnDef(&expr_list, schemas_ctx, true, range))
    }
    return Status::OK();
}

Status BatchModeTransformer::GenWindowUnionList(
    WindowUnionList* window_union_list, PhysicalOpNode* in) {
    if (nullptr == window_union_list || window_union_list->Empty()) {
        DLOG(WARNING) << "Skip GenWindowUnionList when window unions is empty";
        return Status::OK();
    }
    for (auto& window_union : window_union_list->window_unions_) {
        CHECK_STATUS(GenWindow(&window_union.second, in));
    }
    return Status::OK();
}

Status BatchModeTransformer::GenWindowJoinList(
    PhysicalWindowAggrerationNode* window_agg_op, PhysicalOpNode* in) {
    auto window_join_list = &window_agg_op->window_joins_;
    if (nullptr != window_join_list && !window_join_list->Empty()) {
        CHECK_STATUS(window_agg_op->InitJoinList(&plan_ctx_));

        PhysicalOpNode* left = in;
        size_t join_idx = 0;
        auto& joins = window_join_list->window_joins_;
        for (auto& window_join : joins) {
            auto join = &window_join.second;

            // use left ctx
            const SchemasContext* left_ctx = left->schemas_ctx();
            CHECK_STATUS(GenKey(&join->left_key_, left_ctx));
            CHECK_STATUS(GenKey(&join->index_key_, left_ctx));

            // use right ctx
            const SchemasContext* right_ctx = window_join.first->schemas_ctx();
            CHECK_STATUS(GenKey(&join->right_key_, right_ctx));

            // use joined ctx
            PhysicalOpNode* joined_op =
                window_agg_op->joined_op_list_[join_idx];
            left = joined_op;
            CHECK_STATUS(
                GenConditionFilter(&join->condition_, left->schemas_ctx()));
            join_idx += 1;
        }
    }
    return Status::OK();
}

Status BatchModeTransformer::GenRequestWindowUnionList(
    RequestWindowUnionList* window_union_list, PhysicalOpNode* in) {
    if (nullptr == window_union_list || window_union_list->Empty()) {
        DLOG(WARNING)
            << "Skip GenRequestWindowUnionList when window unions is empty";
        return Status::OK();
    }
    for (auto& window_union : window_union_list->window_unions_) {
        CHECK_STATUS(GenRequestWindow(&window_union.second, in));
    }
    return Status::OK();
}

/**
 * Expression is "simple" if only column, const and cast exists.
 */
static bool IsSimpleProjectExpr(const node::ExprNode* expr) {
    if (expr == nullptr) {
        return false;
    }
    switch (expr->GetExprType()) {
        case node::kExprColumnRef:
        case node::kExprPrimary:
            return true;
        case node::kExprCast:
            return IsSimpleProjectExpr(expr->GetChild(0));
        default:
            return false;
    }
}

bool BatchModeTransformer::IsSimpleProject(const ColumnProjects& projects) {
    for (size_t i = 0; i < projects.size(); ++i) {
        if (!IsSimpleProjectExpr(projects.GetExpr(i))) {
            return false;
        }
    }
    return true;
}

Status BatchModeTransformer::CheckTimeOrIntegerOrderColumn(const node::OrderByNode* orders,
                                                           const vm::SchemasContext* schemas_ctx) {
    if (nullptr != orders && !node::ExprListNullOrEmpty(orders->order_expressions_)) {
        CHECK_TRUE(1u == orders->order_expressions_->children_.size(), kPlanError,
                   "Un-support order by multiple expressions currently")
        auto order = dynamic_cast<const node::OrderExpression*>(orders->order_expressions_->children_[0]);
        CHECK_TRUE(nullptr != order && node::kExprColumnRef == order->expr()->expr_type_, common::kPlanError,
                   "Un-support order expression type, expect column expression")

        size_t schema_idx;
        size_t col_idx;
        CHECK_STATUS(schemas_ctx->ResolveColumnRefIndex(
            dynamic_cast<const node::ColumnRefNode*>(order->expr()), &schema_idx, &col_idx));

        auto col_type = schemas_ctx->GetSchema(schema_idx)->Get(col_idx).type();
        switch (col_type) {
            case type::kInt16:
            case type::kInt32:
            case type::kInt64:
            case type::kTimestamp: {
                return Status::OK();
            }
            default: {
                return Status(kPlanError,
                              "Invalid Order column type : " +
                                  hybridse::type::Type_Name(col_type));
            }
        }
    }
    return Status::OK();
}

Status BatchModeTransformer::CheckWindow(
    const node::WindowPlanNode* w_ptr, const vm::SchemasContext* schemas_ctx) {
    CHECK_TRUE(w_ptr != nullptr, common::kPlanError, "NULL Window");
    CHECK_TRUE(!node::ExprListNullOrEmpty(w_ptr->GetKeys()), common::kPlanError,
               "Invalid Window: Do not support window on non-partition");
    CHECK_TRUE(nullptr != w_ptr->GetOrders() &&
                   !node::ExprListNullOrEmpty(w_ptr->GetOrders()->order_expressions_),
               common::kPlanError,
               "Invalid Window: Do not support window on non-order");
    CHECK_STATUS(CheckHistoryWindowFrame(w_ptr));
    CHECK_STATUS(
        CheckTimeOrIntegerOrderColumn(w_ptr->GetOrders(), schemas_ctx));
    return Status::OK();
}
Status BatchModeTransformer::CheckHistoryWindowFrame(
    const node::WindowPlanNode* w_ptr) {
    CHECK_TRUE(w_ptr != nullptr, kPlanError,
               "Invalid Request Window: null window");
    const node::FrameNode* frame = w_ptr->frame_node();
    CHECK_TRUE(frame->frame_type() != node::kFrameRange, kPlanError,
               "Invalid Request Window: Non-support FrameType RANGE,"
               "use ROWS or ROWS_RANGE");
    CHECK_TRUE(
        frame->GetHistoryRangeEnd() <= 0 || frame->GetHistoryRowsEnd() <= 0,
        kPlanError, "Invalid Request Window: end frame can't exceed CURRENT");
    CHECK_TRUE(
        frame->GetHistoryRangeStart() <= 0 && frame->GetHistoryRowsStart() <= 0,
        kPlanError, "Invalid Request Window: start frame can't exceed CURRENT");
    return Status::OK();
}

RequestModeTransformer::RequestModeTransformer(node::NodeManager* node_manager, const std::string& db,
                                               const std::shared_ptr<Catalog>& catalog,
                                               const codec::Schema* parameter_types, ::llvm::Module* module,
                                               udf::UdfLibrary* library, const std::set<size_t>& common_column_indices,
                                               const bool performance_sensitive, const bool cluster_optimized,
                                               const bool enable_batch_request_opt, bool enable_expr_opt)
    : BatchModeTransformer(node_manager, db, catalog, parameter_types, module, library, performance_sensitive,
                           cluster_optimized, enable_expr_opt, true),
      enable_batch_request_opt_(enable_batch_request_opt) {
    batch_request_info_.common_column_indices = common_column_indices;
}

RequestModeTransformer::~RequestModeTransformer() {}

// transform project plan in request mode
Status RequestModeTransformer::TransformProjectPlanOp(
    const node::ProjectPlanNode* node, PhysicalOpNode** output) {
    // sanity check
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError,
               "Input node or output node is null");

    PhysicalOpNode* depend = nullptr;
    CHECK_STATUS(TransformPlanOp(node->GetChildren()[0], &depend));

    std::vector<PhysicalOpNode*> ops;
    for (auto iter = node->project_list_vec_.cbegin();
         iter != node->project_list_vec_.cend(); iter++) {
        hybridse::node::ProjectListNode* project_list =
            dynamic_cast<hybridse::node::ProjectListNode*>(*iter);

        PhysicalOpNode* project_op = nullptr;
        CHECK_STATUS(
            TransformProjectOp(project_list, depend, false, &project_op));
        ops.push_back(project_op);
    }

    CHECK_TRUE(!ops.empty(), kPlanError,
               "Fail transform project op: empty projects");

    if (ops.size() == 1) {
        *output = ops[0];
        return Status::OK();
    }

    PhysicalRequestJoinNode* join = nullptr;
    CHECK_STATUS(CreateOp<PhysicalRequestJoinNode>(
        &join, ops[0], ops[1], ::hybridse::node::kJoinTypeConcat));

    for (size_t i = 2; i < ops.size(); ++i) {
        PhysicalRequestJoinNode* new_join = nullptr;
        CHECK_STATUS(CreateOp<PhysicalRequestJoinNode>(
            &new_join, join, ops[i], ::hybridse::node::kJoinTypeConcat));
        join = new_join;
    }

    auto project_list = node_manager_->MakeProjectListPlanNode(nullptr, false);
    uint32_t pos = 0;
    for (auto iter = node->pos_mapping_.cbegin();
         iter != node->pos_mapping_.cend(); iter++) {
        auto sub_project_list = dynamic_cast<node::ProjectListNode*>(
            node->project_list_vec_[iter->first]);

        auto project_node = dynamic_cast<node::ProjectNode*>(
            sub_project_list->GetProjects().at(iter->second));
        if (node::kExprAll == project_node->GetExpression()->expr_type_) {
            auto all_expr =
                dynamic_cast<node::AllNode*>(project_node->GetExpression());
            if (all_expr->children_.empty()) {
                // expand all expression if needed
                for (auto column : *depend->GetOutputSchema()) {
                    all_expr->children_.push_back(
                        node_manager_->MakeColumnRefNode(
                            column.name(), all_expr->GetRelationName()));
                }
            }
            project_list->AddProject(
                node_manager_->MakeRowProjectNode(pos, "*", all_expr));
        } else {
            project_list->AddProject(node_manager_->MakeRowProjectNode(
                pos, project_node->GetName(),
                node_manager_->MakeColumnRefNode(project_node->GetName(), "")));
        }
        pos++;
    }
    return CreatePhysicalProjectNode(kRowProject, join, project_list, false,
                                     output);
}

Status RequestModeTransformer::TransformJoinOp(const node::JoinPlanNode* node,
                                               PhysicalOpNode** output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError,
               "Input node or output node is null");
    PhysicalOpNode* left = nullptr;

    CHECK_STATUS(TransformPlanOp(node->GetChildren()[0], &left));

    PhysicalOpNode* right = nullptr;
    CHECK_STATUS(TransformPlanOp(node->GetChildren()[1], &right));

    PhysicalRequestJoinNode* request_join_op = nullptr;
    CHECK_STATUS(CreateOp<PhysicalRequestJoinNode>(
        &request_join_op, left, right, node->join_type_, node->orders_,
        node->condition_));

    CHECK_STATUS(CheckTimeOrIntegerOrderColumn(node->orders_,
                                               request_join_op->schemas_ctx()));
    *output = request_join_op;
    return Status::OK();
}

Status RequestModeTransformer::TransformScanOp(const node::TablePlanNode* node,
                                               PhysicalOpNode** output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError,
               "Input node or output node is null");

    if (node->IsPrimary()) {
        auto table = catalog_->GetTable(db_, node->table_);
        CHECK_TRUE(table != nullptr, common::kTableNotFound,
                   "Fail to transform data_provider op: table " + db_ + "." +
                       node->table_ + " not exist!");

        PhysicalRequestProviderNode* provider = nullptr;
        CHECK_STATUS(CreateOp<PhysicalRequestProviderNode>(&provider, table));
        *output = provider;
        request_schema_ = *(*output)->GetOutputSchema();
        request_name_ = table->GetName();
        return Status::OK();
    } else {
        return BatchModeTransformer::TransformScanOp(node, output);
    }
}
Status RequestModeTransformer::ValidatePrimaryPath(
    PhysicalOpNode* node, PhysicalOpNode** primary_source) {
    CHECK_TRUE(node != NULL, kPlanError, "NULL Physical Node");

    switch (node->GetOpType()) {
        case vm::kPhysicalOpDataProvider: {
            CHECK_TRUE(kProviderTypeRequest ==
                           dynamic_cast<PhysicalDataProviderNode*>(node)
                               ->provider_type_,
                       kPlanError, "Non-support primary source node ",
                       node->GetTreeString())
            *primary_source = node;
            return Status::OK();
        }
        case vm::kPhysicalOpJoin:
        case vm::kPhysicalOpUnion:
        case vm::kPhysicalOpPostRequestUnion:
        case vm::kPhysicalOpRequestUnion:
            // TODO(chenjing): add specific validation code for request union op
        case vm::kPhysicalOpRequestJoin: {
            vm::PhysicalOpNode* left_primary_source = nullptr;
            CHECK_STATUS(
                ValidatePrimaryPath(node->GetProducer(0), &left_primary_source))
            CHECK_TRUE(
                nullptr != left_primary_source, kPlanError,
                "primary path validate fail: left primary source is null")
            // Case 1:
            // binary_node
            //    + Left - PrimarySource
            //    + Right - TableProvider
            if (isSourceFromTableOrPartition(node->GetProducer(1))) {
                *primary_source = left_primary_source;
                return Status::OK();
            }

            vm::PhysicalOpNode* right_primary_source = nullptr;
            CHECK_STATUS(ValidatePrimaryPath(node->GetProducer(1),
                                             &right_primary_source))
            CHECK_TRUE(
                nullptr != right_primary_source, kPlanError,
                "primary path validate fail: right primary source is null")
            // Case 2:
            // binary_node
            //      + Left <-- SamePrimarySource1
            //      + Right <-- SamePrimarySource1
            CHECK_TRUE(
                left_primary_source->Equals(right_primary_source), kPlanError,
                "primary path validate fail: left path and right path has "
                "different source")
            *primary_source = left_primary_source;
            return Status::OK();
        }
        case vm::kPhysicalOpConstProject: {
            CHECK_TRUE(false, kPlanError,
                       "Non-support Const Project in request mode",
                       node->GetTreeString());
            break;
        }
        default: {
            CHECK_TRUE(node->GetProducerCnt() == 1, kPlanError,
                       "Non-support Op ", PhysicalOpTypeName(node->GetOpType()))
            CHECK_STATUS(
                ValidatePrimaryPath(node->GetProducer(0), primary_source));
            return Status::OK();
        }
    }
    return Status::OK();
}
Status RequestModeTransformer::TransformProjectOp(
    node::ProjectListNode* project_list, PhysicalOpNode* depend,
    bool append_input, PhysicalOpNode** output) {
    PhysicalOpNode* new_depend = depend;
    if (nullptr != project_list->w_ptr_) {
        CHECK_STATUS(
            TransformWindowOp(depend, project_list->w_ptr_, &new_depend));
    }
    switch (new_depend->GetOutputType()) {
        case kSchemaTypeRow:
            CHECK_TRUE(!project_list->has_agg_project_, kPlanError, "Non-support aggregation project on request row")
            return CreatePhysicalProjectNode(
                kRowProject, new_depend, project_list, append_input, output);
        case kSchemaTypeGroup:
            return CreatePhysicalProjectNode(kGroupAggregation, new_depend,
                                             project_list, append_input,
                                             output);
        case kSchemaTypeTable:
            if (project_list->has_agg_project_) {
                return CreatePhysicalProjectNode(kAggregation, new_depend,
                                                 project_list, append_input,
                                                 output);
            } else {
                return CreatePhysicalProjectNode(kTableProject, new_depend,
                                                 project_list, append_input,
                                                 output);
            }
        default:
            return Status(kPlanError, "Unknown op output type");
    }
}

void RequestModeTransformer::ApplyPasses(PhysicalOpNode* node,
                                         PhysicalOpNode** output) {
    PhysicalOpNode* optimized = nullptr;
    this->BatchModeTransformer::ApplyPasses(node, &optimized);
    if (optimized == nullptr) {
        *output = node;
        DLOG(WARNING) << "Final optimized result is null";
        return;
    }
    if (!enable_batch_request_opt_ ||
        batch_request_info_.common_column_indices.empty()) {
        *output = optimized;
        return;
    }
    LOG(INFO) << "Before batch request optimization:\n" << *optimized;
    PhysicalOpNode* batch_request_plan = nullptr;
    CommonColumnOptimize batch_request_optimizer(
        batch_request_info_.common_column_indices);
    Status status = batch_request_optimizer.Apply(
        this->GetPlanContext(), optimized, &batch_request_plan);
    if (!status.isOK()) {
        DLOG(WARNING) << "Fail to perform batch request optimization: "
                     << status;
        *output = optimized;
        return;
    }
    LOG(INFO) << "After batch request optimization:\n" << *batch_request_plan;
    batch_request_optimizer.ExtractCommonNodeSet(
        &batch_request_info_.common_node_set);
    batch_request_info_.output_common_column_indices =
        batch_request_optimizer.GetOutputCommonColumnIndices();
    *output = batch_request_plan;
    return;
}

}  // namespace vm
}  // namespace hybridse
