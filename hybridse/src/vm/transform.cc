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

#include <set>
#include <stack>
#include <unordered_map>

#include "absl/cleanup/cleanup.h"
#include "codegen/context.h"
#include "codegen/fn_ir_builder.h"
#include "codegen/fn_let_ir_builder.h"
#include "passes/expression/expr_pass.h"
#include "passes/lambdafy_projects.h"
#include "passes/physical/batch_request_optimize.h"
#include "passes/physical/cluster_optimized.h"
#include "passes/physical/condition_optimized.h"
#include "passes/physical/group_and_sort_optimized.h"
#include "passes/physical/left_join_optimized.h"
#include "passes/physical/limit_optimized.h"
#include "passes/physical/long_window_optimized.h"
#include "passes/physical/simple_project_optimized.h"
#include "passes/physical/split_aggregation_optimized.h"
#include "passes/physical/transform_up_physical_pass.h"
#include "passes/physical/window_column_pruning.h"
#include "plan/planner.h"
#include "vm/physical_op.h"
#include "vm/schemas_context.h"
#include "vm/internal/node_helper.h"

namespace hybridse {
namespace vm {

using ::hybridse::base::Status;
using ::hybridse::common::kPlanError;
using ::hybridse::common::kCodegenError;
using hybridse::passes::ClusterOptimized;
using hybridse::passes::CommonColumnOptimize;
using hybridse::passes::ConditionOptimized;
using hybridse::passes::GroupAndSortOptimized;
using hybridse::passes::LeftJoinOptimized;
using hybridse::passes::LimitOptimized;
using hybridse::passes::PhysicalPlanPassType;
using hybridse::passes::SimpleProjectOptimized;
using hybridse::passes::WindowColumnPruning;
using hybridse::passes::LongWindowOptimized;
using hybridse::passes::SplitAggregationOptimized;

std::ostream& operator<<(std::ostream& output,
                         const hybridse::vm::LogicalOp& thiz) {
    return output << *(thiz.node_);
}

// Fixup window def over simpleproject(SimpleProject...(last join))
// in SQL window's column name is based on simpleproject, where after optimization, it is based on a physical table
//
// since in request mode, window operation is doing earlier than last join
// this helper meothod fixes the window definition by revert all column names
// where any one or more simple projection did
static Status FixupWindowOverSimpleNLastJoin(const node::WindowPlanNode* w_ptr, const SchemasContext* window_depend_sc,
                                             const SchemasContext* left_join_sc);

BatchModeTransformer::BatchModeTransformer(node::NodeManager* node_manager, const std::string& db,
                                           const std::shared_ptr<Catalog>& catalog,
                                           const codec::Schema* parameter_types, ::llvm::Module* module,
                                           const udf::UdfLibrary* library, bool cluster_optimized_mode,
                                           bool enable_expr_opt, bool enable_window_parallelization,
                                           bool enable_window_column_pruning,
                                           const std::unordered_map<std::string, std::string>* options)
    : node_manager_(node_manager),
      db_(db),
      catalog_(catalog),
      plan_ctx_(node_manager, library, db, catalog, parameter_types, enable_expr_opt, options),
      module_(module),
      id_(0),
      cluster_optimized_mode_(cluster_optimized_mode),
      enable_batch_window_parallelization_(enable_window_parallelization),
      enable_batch_window_column_pruning_(enable_window_column_pruning),
      library_(library) {}

BatchModeTransformer::~BatchModeTransformer() {}

Status BatchModeTransformer::TransformPlanOp(const node::PlanNode* node, PhysicalOpNode** output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError, "Input node or output node is null");

    LogicalOp logical_op = LogicalOp(node, closure_);
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
        case node::kPlanTypeLoadData: {
            CHECK_STATUS(TransformLoadDataOp(dynamic_cast<const ::hybridse::node::LoadDataPlanNode*>(node),
                                             &op));
            break;
        }
        case node::kPlanTypeDelete: {
            CHECK_STATUS(TransformDeleteOp(dynamic_cast<const ::hybridse::node::DeletePlanNode*>(node), &op));
            break;
        }
        case node::kPlanTypeWithClauseEntry: {
            CHECK_STATUS(
                TransformWithClauseEntry(dynamic_cast<const ::hybridse::node::WithClauseEntryPlanNode*>(node), &op));
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
            switch (project_op->project_type_) {
                case kGroupAggregation: {
                    // Code generation for having condition
                    CHECK_STATUS(
                        GenHavingFilter(&(dynamic_cast<PhysicalGroupAggrerationNode*>(node))->having_condition_,
                                        project_op->producers()[0]->schemas_ctx()));
                    break;
                }
                case kAggregation: {
                    // Code generation for having condition
                    CHECK_STATUS(GenHavingFilter(&(dynamic_cast<PhysicalAggregationNode*>(node))->having_condition_,
                                                    project_op->producers()[0]->schemas_ctx()));
                    break;
                }
                case kWindowAggregation: {
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
                default: {
                    break;
                }
            }
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
        case kPhysicalOpRequestAggUnion: {
            auto request_union_op =
                dynamic_cast<PhysicalRequestAggUnionNode*>(node);
            CHECK_STATUS(GenRequestWindow(&request_union_op->window_,
                                          node->producers()[0]));
            CHECK_STATUS(GenRequestWindow(&request_union_op->agg_window_,
                                          node->producers()[2]));
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
        CHECK_STATUS(InstantiateLLVMFunction(fn_info), "Instantiate ", i,
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

    CHECK_STATUS(CompleteProjectList(node, depend));
    if (1 == node->project_list_vec_.size()) {
        return TransformProjectOp(dynamic_cast<hybridse::node::ProjectListNode*>(node->project_list_vec_[0]), depend,
                                  false, output);
    }

    std::vector<PhysicalOpNode*> ops;
    for (auto iter = node->project_list_vec_.cbegin();
         iter != node->project_list_vec_.cend(); iter++) {
        hybridse::node::ProjectListNode* project_list =
            dynamic_cast<hybridse::node::ProjectListNode*>(*iter);

        PhysicalOpNode* project_op = nullptr;
        CHECK_STATUS(TransformProjectOp(project_list, depend, false, &project_op));
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
    CHECK_TRUE(!node->project_list_vec_.empty(), kPlanError, "Fail transform project op: empty projects");

    CHECK_STATUS(CompleteProjectList(node, depend));

    if (1 == node->project_list_vec_.size()) {
        return TransformProjectOp(
            dynamic_cast<hybridse::node::ProjectListNode*>(
                node->project_list_vec_[0]),
            depend, false, output);
    }

    // 处理project_list_vec_[1...N-1], 串联执行windowAggWithAppendInput
    std::vector<PhysicalOpNode*> ops;
    for (size_t i = node->project_list_vec_.size() - 1; i > 0; i--) {
        hybridse::node::ProjectListNode* project_list =
            dynamic_cast<hybridse::node::ProjectListNode*>(node->project_list_vec_[i]);
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
        first_project_list->GetW(), first_project_list->HasAggProject());
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

// fullfill project list expr early
// this adds correct column list for AllExpr('*'), cuz for serial mode,
// AllExpr wants columns from original depends, instead of e.g. the windowAggWithAppendInput Node
Status BatchModeTransformer::CompleteProjectList(const node::ProjectPlanNode* project_node,
                                                 PhysicalOpNode* depend) const {
    for (auto ele : project_node->project_list_vec_) {
        auto& projects = dynamic_cast<node::ProjectListNode*>(ele)->GetProjects();
        for (auto iter = projects.cbegin(); iter != projects.cend(); iter++) {
            auto project_node = dynamic_cast<node::ProjectNode*>(*iter);
            auto expr = project_node->GetExpression();
            CHECK_TRUE(expr != nullptr, kPlanError, "Invalid project: expression is null");
            if (node::kExprAll == expr->expr_type_) {
                auto all_expr = dynamic_cast<node::AllNode*>(expr);
                // we assume children of AllExpr will fullfilled once, it is not
                // expected to have AllExpr's children already fullfilled
                CHECK_TRUE(all_expr->children_.empty(), kPlanError, "all expr already fullfilled children expr");
                CHECK_TRUE(depend != nullptr, kPlanError,
                           "try to '*' expand all columns from depend but depend is null");
                const auto& relation_name = all_expr->GetRelationName();
                const auto& db_name = all_expr->GetDBName();
                // expand *
                auto* schemas_ctx = depend->schemas_ctx();
                for (size_t slice = 0; slice < schemas_ctx->GetSchemaSourceSize(); slice++) {
                    // t1.* or db.t1.* only selects columns of one table
                    auto schema_source = schemas_ctx->GetSchemaSource(slice);
                    if (!relation_name.empty() && relation_name != schema_source->GetSourceName()) {
                        continue;
                    }

                    if (!db_name.empty() && db_name != schema_source->GetSourceDB()) {
                        continue;
                    }

                    for (size_t k = 0; k < schema_source->size(); ++k) {
                        auto col_name = schema_source->GetColumnName(k);
                        std::string col_db = "";
                        // db name will omitted if it is equal to default db
                        if (schema_source->GetSourceDB() != schemas_ctx->GetDefaultDBName()) {
                            col_db = schema_source->GetSourceDB();
                        }
                        auto col_ref =
                            node_manager_->MakeColumnRefNode(col_name, schema_source->GetSourceName(), col_db);
                        all_expr->children_.push_back(col_ref);
                    }
                }
            }
        }
    }
    return Status::OK();
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

Status RequestModeTransformer::CreateRequestUnionNode(
    PhysicalOpNode* request, PhysicalOpNode* right,
    const std::string& db_name,
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
            CHECK_STATUS(plan_ctx_.GetRequestSourceID(db_name, primary_name, col_name,
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
        CHECK_STATUS(CreateOp<PhysicalRequestUnionNode>(&request_union_op, left, right, partition));
    } else {
        CHECK_STATUS(CreateOp<PhysicalRequestUnionNode>(&request_union_op, left, right, window_plan));
    }
    *output = request_union_op;
    return Status::OK();
}

Status RequestModeTransformer::TransformWindowOp(PhysicalOpNode* depend,
                                               const node::WindowPlanNode* w_ptr,
                                               PhysicalOpNode** output) {
    // sanity check
    CHECK_TRUE(depend != nullptr && output != nullptr, kPlanError,
               "Depend node or output node is null");
    CHECK_STATUS(CheckWindow(w_ptr, depend->schemas_ctx()));

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
            auto db_name = data_op->table_handler_->GetDatabase();
            auto table = catalog_->GetTable(db_name, name);
            CHECK_TRUE(table != nullptr, kPlanError,
                       "Fail to transform data provider op: table " + name +
                           "not exists");
            PhysicalTableProviderNode* right = nullptr;
            CHECK_STATUS(CreateOp<PhysicalTableProviderNode>(&right, table));

            PhysicalRequestUnionNode* request_union_op = nullptr;
            CHECK_STATUS(CreateRequestUnionNode(
                data_op, right, table->GetDatabase(), table->GetName(), table->GetSchema(), nullptr,
                w_ptr, &request_union_op));

            if (!w_ptr->union_tables().empty()) {
                for (auto iter = w_ptr->union_tables().cbegin();
                     iter != w_ptr->union_tables().cend(); iter++) {
                    PhysicalOpNode* union_table_op;
                    CHECK_STATUS(TransformPlanOp(*iter, &union_table_op));
                    PhysicalRenameNode* rename_union_op = nullptr;
                    CHECK_STATUS(CreateOp<PhysicalRenameNode>(&rename_union_op, union_table_op,
                                                              depend->schemas_ctx()->GetName()));
                    CHECK_TRUE(request_union_op->AddWindowUnion(rename_union_op),
                               kPlanError,
                               "Fail to add request window union table");
                }
            }
            *output = request_union_op;
            break;
        }
        case kPhysicalOpRequestJoin: {
            auto* join_op = dynamic_cast<PhysicalRequestJoinNode*>(depend);
            CHECK_TRUE(join_op != nullptr, kPlanError);
            return OptimizeRequestJoinAsWindowProducer(join_op, join_op->schemas_ctx(), w_ptr, output);
        }
        case kPhysicalOpSimpleProject: {
            auto* simple_project = dynamic_cast<PhysicalSimpleProjectNode*>(depend);
            CHECK_TRUE(simple_project != nullptr, kPlanError);
            return OptimizeSimpleProjectAsWindowProducer(simple_project, simple_project->schemas_ctx(), w_ptr, output);
        }
        default: {
            FAIL_STATUS(kPlanError, "Do not support window on\n" + depend->GetTreeString());
        }
    }
    return Status::OK();
}

Status RequestModeTransformer::OptimizeSimpleProjectAsWindowProducer(PhysicalSimpleProjectNode* prj_node,
                                                                     const SchemasContext* window_depend_sc,
                                                                     const node::WindowPlanNode* w_ptr,
                                                                     PhysicalOpNode** output) {
    // - SimpleProject(DataProvider) -> RequestUnion(Request, DataSource)
    // - SimpleProject(RequestJoin) -> Join(RequestUnion, DataSource)
    // - SimpleProject(SimpleProject)
    // - SimpleProject(Rename)
    auto* depend = prj_node->GetProducer(0);
    auto op_type = depend->GetOpType();

    PhysicalOpNode* new_depend = nullptr;

    // TODO(ace): rename(last join) unsupported
    std::shared_ptr<TableHandler> table = nullptr;

    auto func = [this, &table](PhysicalOpNode* in, PhysicalOpNode** out) {
        // PhysicalRequestProviderNode -> PhysicalTableProviderNode
        switch (in->GetOpType()) {
            case kPhysicalOpDataProvider: {
                auto* data_op = dynamic_cast<PhysicalRequestProviderNode*>(in);
                CHECK_TRUE(data_op != nullptr, kPlanError, "not PhysicalRequestProviderNode: ", in->GetTreeString());

                auto name = data_op->table_handler_->GetName();
                auto db_name = data_op->table_handler_->GetDatabase();
                db_name = db_name.empty() ? db_ : db_name;
                auto tb = catalog_->GetTable(db_name, name);
                CHECK_TRUE(tb != nullptr, kPlanError,
                           "Fail to transform data provider op: table " + name + "not exists");
                CHECK_TRUE(table == nullptr, kPlanError, "multiple table provider found in single window");
                table = tb;

                PhysicalTableProviderNode* right = nullptr;
                CHECK_STATUS(CreateOp<PhysicalTableProviderNode>(&right, tb));
                *out = right;
                break;
            }
            case kPhysicalOpJoin:
            case kPhysicalOpRequestJoin: {
                FAIL_STATUS(common::kPhysicalPlanError, "Unsupported window over (rename(last join))");
            }
            default: {
                *out = in;
                break;
            }
        }
        return Status::OK();
    };
    switch (op_type) {
        case kPhysicalOpRename: {
            PhysicalOpNode* right_table = nullptr;
            CHECK_STATUS(internal::MapNode(&plan_ctx_, prj_node, &right_table, func));

            // request union
            PhysicalRequestUnionNode* request_union_op = nullptr;
            CHECK_STATUS(CreateRequestUnionNode(prj_node, right_table, table->GetDatabase(), table->GetName(),
                                                table->GetSchema(), nullptr, w_ptr, &request_union_op));
            if (!w_ptr->union_tables().empty()) {
                for (auto iter = w_ptr->union_tables().cbegin(); iter != w_ptr->union_tables().cend(); iter++) {
                    PhysicalOpNode* union_table_op;
                    CHECK_STATUS(TransformPlanOp(*iter, &union_table_op));
                    PhysicalRenameNode* rename_union_op = nullptr;
                    CHECK_STATUS(CreateOp<PhysicalRenameNode>(&rename_union_op, union_table_op,
                                                              prj_node->schemas_ctx()->GetName()));
                    CHECK_TRUE(request_union_op->AddWindowUnion(rename_union_op), kPlanError,
                               "Fail to add request window union table");
                }
            }
            *output = request_union_op;

            return Status::OK();
        }
        case kPhysicalOpDataProvider: {
            auto data_op = dynamic_cast<PhysicalDataProviderNode*>(prj_node->GetProducer(0));
            CHECK_TRUE(data_op != nullptr, kPlanError, "not PhysicalDataProviderNode");
            CHECK_TRUE(data_op->provider_type_ == kProviderTypeRequest, kPlanError,
                       "Do not support window on non-request input");

            auto name = data_op->table_handler_->GetName();
            auto db_name = data_op->table_handler_->GetDatabase();
            db_name = db_name.empty() ? db_ : db_name;
            auto table = catalog_->GetTable(db_name, name);
            CHECK_TRUE(table != nullptr, kPlanError,
                       "Fail to transform data provider op: table " + name + "not exists");

            PhysicalTableProviderNode* right = nullptr;
            CHECK_STATUS(CreateOp<PhysicalTableProviderNode>(&right, table));

            // right side simple project
            PhysicalSimpleProjectNode* right_simple_project = nullptr;
            CHECK_STATUS(CreateOp<PhysicalSimpleProjectNode>(&right_simple_project, right, prj_node->project()));

            // request union
            PhysicalRequestUnionNode* request_union_op = nullptr;
            CHECK_STATUS(CreateRequestUnionNode(prj_node, right_simple_project, table->GetDatabase(), table->GetName(),
                                                table->GetSchema(), nullptr, w_ptr, &request_union_op));
            if (!w_ptr->union_tables().empty()) {
                for (auto iter = w_ptr->union_tables().cbegin(); iter != w_ptr->union_tables().cend(); iter++) {
                    PhysicalOpNode* union_table_op;
                    CHECK_STATUS(TransformPlanOp(*iter, &union_table_op));
                    PhysicalRenameNode* rename_union_op = nullptr;
                    CHECK_STATUS(CreateOp<PhysicalRenameNode>(&rename_union_op, union_table_op,
                                                              prj_node->schemas_ctx()->GetName()));
                    CHECK_TRUE(request_union_op->AddWindowUnion(rename_union_op), kPlanError,
                               "Fail to add request window union table");
                }
            }
            *output = request_union_op;
            return Status::OK();
        }
        case kPhysicalOpRequestJoin: {
            auto join_op = dynamic_cast<PhysicalRequestJoinNode*>(prj_node->GetProducer(0));
            CHECK_TRUE(join_op != nullptr, kPlanError, "not PhysicalRequestJoinNode");
            CHECK_STATUS(OptimizeRequestJoinAsWindowProducer(join_op, window_depend_sc, w_ptr, &new_depend));
            break;
        }
        case kPhysicalOpSimpleProject: {
            auto simple_prj_node = dynamic_cast<PhysicalSimpleProjectNode*>(prj_node->GetProducer(0));
            CHECK_TRUE(simple_prj_node != nullptr, kPlanError, "not PhysicalSimpleProjectNode");
            CHECK_STATUS(OptimizeSimpleProjectAsWindowProducer(simple_prj_node, window_depend_sc, w_ptr, &new_depend));
            break;
        }
        default: {
            FAIL_STATUS(kPlanError, "Do not support window on\n", prj_node->GetTreeString());
        }
    }
    PhysicalSimpleProjectNode* simple_proj = nullptr;
    CHECK_STATUS(CreateOp<PhysicalSimpleProjectNode>(&simple_proj, new_depend, prj_node->project()));
    *output = simple_proj;
    return Status::OK();
}

// Optimize
//   RequestJoin(Request(left_table), DataSource(right_table))
// ->
//   Join
//     RequestUnion
//       Request
//       DataSource(left_table)
//     DataSource(right_table)
//
// \param join_op: RequestJoinNode def
// \param w_ptr: window def
// \param window_depend_sc: schema context of producer or window node.
//        It may not the same with the context of `join_op`
// \param output: transformed Join node
Status RequestModeTransformer::OptimizeRequestJoinAsWindowProducer(PhysicalRequestJoinNode* join_op,
                                                                   const SchemasContext* window_depend_sc,
                                                                   const node::WindowPlanNode* w_ptr,
                                                                   PhysicalOpNode** output) {
    switch (join_op->join().join_type()) {
        case node::kJoinTypeLeft:
        case node::kJoinTypeLast: {
            CHECK_STATUS(
                FixupWindowOverSimpleNLastJoin(w_ptr, window_depend_sc, join_op->GetProducer(0)->schemas_ctx()));

            CHECK_TRUE(join_op->GetProducer(0)->GetOpType() == kPhysicalOpDataProvider, kPlanError,
                       "Fail to handler window with request last join, left isn't a table provider")

            auto request_op = dynamic_cast<PhysicalDataProviderNode*>(join_op->producers()[0]);
            auto name = request_op->table_handler_->GetName();
            auto db_name = request_op->table_handler_->GetDatabase();
            if (db_name.empty()) {
                db_name = db_;
            }
            auto table = catalog_->GetTable(db_name, name);
            CHECK_TRUE(table != nullptr, kPlanError,
                       "Fail to transform data provider op: table " + name + "not exists");
            PhysicalTableProviderNode* right = nullptr;
            CHECK_STATUS(CreateOp<PhysicalTableProviderNode>(&right, table));

            PhysicalRequestUnionNode* request_union_op = nullptr;
            CHECK_STATUS(CreateRequestUnionNode(request_op, right, db_name, name, table->GetSchema(), nullptr, w_ptr,
                                                &request_union_op));
            if (!w_ptr->union_tables().empty()) {
                for (auto iter = w_ptr->union_tables().cbegin(); iter != w_ptr->union_tables().cend(); iter++) {
                    PhysicalOpNode* union_table_op;
                    CHECK_STATUS(TransformPlanOp(*iter, &union_table_op));
                    PhysicalRenameNode* rename_union_op = nullptr;
                    CHECK_STATUS(CreateOp<PhysicalRenameNode>(&rename_union_op, union_table_op,
                                                              join_op->schemas_ctx()->GetName()));
                    CHECK_TRUE(request_union_op->AddWindowUnion(rename_union_op), kPlanError,
                               "Fail to add request window union table");
                }
            }
            PhysicalJoinNode* join_output = nullptr;
            CHECK_STATUS(
                CreateOp<PhysicalJoinNode>(&join_output, request_union_op, join_op->producers()[1], join_op->join_));
            *output = join_output;
            break;
        }
        default: {
            return Status(kPlanError, "Non-support join type");
        }
    }

    return Status::OK();
}

Status FixupWindowOverSimpleNLastJoin(const node::WindowPlanNode* w_ptr, const SchemasContext* window_depend_sc,
                                      const SchemasContext* left_join_sc) {
    const node::OrderByNode* orders = w_ptr->GetOrders();
    const node::ExprListNode* groups = w_ptr->GetKeys();

    if (!node::ExprListNullOrEmpty(groups)) {
        std::vector<const node::ExprNode*> exprs;
        CHECK_STATUS(vm::DoSearchExprDependentColumns(groups, &exprs))
        for (auto expr : exprs) {
            switch (expr->GetExprType()) {
                case node::kExprColumnRef: {
                    auto ref = dynamic_cast<const node::ColumnRefNode*>(expr);
                    CHECK_TRUE(ref != nullptr, kPlanError, "not ColumnRefNode");

                    size_t id = 0;
                    CHECK_STATUS(window_depend_sc->ResolveColumnID(ref->GetDBName(), ref->GetRelationName(),
                                                                   ref->GetColumnName(), &id));
                    std::string name;
                    CHECK_STATUS(
                        left_join_sc->ResolveColumnNameByID(id, &name),
                        "Fail to handle window: window partition expression should belong to left table of join");
                    const_cast<node::ColumnRefNode*>(ref)->SetColumnName(name);

                    break;
                }
                default: {
                    break;
                }
            }
        }
    }
    if (nullptr != orders && !node::ExprListNullOrEmpty(orders->order_expressions_)) {
        std::vector<const node::ExprNode*> exprs;
        CHECK_STATUS(vm::DoSearchExprDependentColumns(orders->order_expressions(), &exprs));
        for (auto expr : exprs) {
            switch (expr->GetExprType()) {
                case node::kExprColumnRef: {
                    auto ref = dynamic_cast<const node::ColumnRefNode*>(expr);

                    size_t id = 0;
                    CHECK_STATUS(window_depend_sc->ResolveColumnID(ref->GetDBName(), ref->GetRelationName(),
                                                                   ref->GetColumnName(), &id));
                    std::string name;
                    CHECK_STATUS(left_join_sc->ResolveColumnNameByID(id, &name),
                                 "Fail to handle window: window order expression should belong to left table of join");
                    const_cast<node::ColumnRefNode*>(ref)->SetColumnName(name);

                    break;
                }
                default: {
                    break;
                }
            }
        }
    }
    return base::Status::OK();
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

    CHECK_STATUS(CheckTimeOrIntegerOrderColumn(node->orders_, join_op->schemas_ctx()));

    *output = join_op;
    return Status::OK();
}

Status BatchModeTransformer::TransformGroupOp(const node::GroupPlanNode* node,
                                              PhysicalOpNode** output) {
    PhysicalOpNode* left = nullptr;
    CHECK_STATUS(TransformPlanOp(node->GetChildren()[0], &left));

    PhysicalGroupNode* group_op = nullptr;
    CHECK_STATUS(CreateOp<PhysicalGroupNode>(&group_op, left, node->by_list_));
    *output = group_op;
    return Status::OK();
}
Status RequestModeTransformer::TransformGroupOp(const node::GroupPlanNode* node,
                                              PhysicalOpNode** output) {
    FAIL_STATUS(kPlanError, "Non-support GROUP BY OP in Online serving");
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

    if (node->db_.empty()) {
        auto res = ResolveCTERef(node->table_, false);
        if (res.ok()) {
            *output = res.value();
            return Status::OK();
        }

        if (!absl::IsNotFound(res.status())) {
            FAIL_STATUS(common::kPlanError, res.status().ToString());
        }
    }

    std::string db_name = node->db_.empty() ? db_ : node->db_;
    auto table = catalog_->GetTable(db_name, node->table_);
    CHECK_TRUE(table != nullptr, kPlanError,
               "Fail to transform data provider op: table ", node->GetPathString(),
                   " not exists in database [", db_name, "]");

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

Status BatchModeTransformer::TransformDeleteOp(const node::DeletePlanNode* node, PhysicalOpNode** output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError, "Input node or output node is null");
    PhysicalDeleteNode* delete_op = nullptr;
    CHECK_STATUS(CreateOp<PhysicalDeleteNode>(&delete_op, node->GetTarget(), node->GetJobId()));
    *output = delete_op;
    return Status::OK();
}

Status BatchModeTransformer::TransformCreateTableOp(const node::CreatePlanNode* create, PhysicalOpNode** output) {
    CHECK_TRUE(create != nullptr && output != nullptr, kPlanError, "Input node or output node is null");
    PhysicalCreateTableNode* n = nullptr;
    CHECK_STATUS(CreateOp<PhysicalCreateTableNode>(&n, create));
    *output = n;
    return Status::OK();
}

Status BatchModeTransformer::TransformQueryPlan(const ::hybridse::node::QueryPlanNode* node,
                                                ::hybridse::vm::PhysicalOpNode** output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError, "Input node or output node is null");

    auto* parent_closure = closure_;
    internal::CTEEnv env;
    // empty new Closure may pushed as a repect to with clause
    PushCTEEnv(env);

    absl::Cleanup pop_closure = [this]() {
            auto s = PopCTEs();
            if (!s.isOK()) {
                LOG(ERROR) << s;
            }
    };

    for (auto with_entry : node->with_clauses_) {
        // lazy evaluate WITH clause

        // update CTE environment for next iterating
        if (env.ctes.contains(with_entry->alias_)) {
            FAIL_STATUS(common::kPlanError, "multiple CTEs in the same WITH clause can't have same name: ",
                        with_entry->alias_);
        }

        auto* cte_entry = node_manager_->MakeObj<internal::CTEEntry>(with_entry, closure_);
        env.ctes.emplace(with_entry->alias_, cte_entry);
        ReplaceClosure(node_manager_->MakeObj<internal::Closure>(parent_closure, env));
    }

    return TransformPlanOp(node->GetChildren()[0], output);
}

Status BatchModeTransformer::TransformSelectIntoOp(const node::SelectIntoPlanNode* node, PhysicalOpNode* child,
                                                   PhysicalOpNode** output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError, "Input node or output node is null");
    PhysicalSelectIntoNode* select_into_op = nullptr;
    // db.table should be checked when you get the physical plan
    CHECK_STATUS(CreateOp<PhysicalSelectIntoNode>(&select_into_op, child, node->QueryStr(), node->OutFile(),
                                                  node->Options(), node->ConfigOptions()))
    *output = select_into_op;
    return Status::OK();
}

Status BatchModeTransformer::TransformLoadDataOp(const node::LoadDataPlanNode* node, PhysicalOpNode** output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError, "Input node or output node is null");
    PhysicalLoadDataNode* load_data_op = nullptr;
    // db.table should be checked when you get the physical plan
    CHECK_STATUS(CreateOp<PhysicalLoadDataNode>(&load_data_op, node->File(), node->Db(), node->Table(), node->Options(),
                                                node->ConfigOptions()));
    *output = load_data_op;
    return Status::OK();
}

bool BatchModeTransformer::AddPass(PhysicalPlanPassType type) {
    DLOG(INFO) << "AddPass " << passes::PhysicalPlanPassTypeName(type);
    passes.push_back(type);
    return true;
}

Status ExtractProjectInfos(const node::PlanNodeList& projects, const node::FrameNode* primary_frame,
                           ColumnProjects* output) {
    for (auto plan_node : projects) {
        auto pp_node = dynamic_cast<node::ProjectNode*>(plan_node);
        CHECK_TRUE(pp_node != nullptr, kPlanError, "project node is null");
        auto expr = pp_node->GetExpression();
        CHECK_TRUE(expr != nullptr, kPlanError, "expr in project node is null");
        if (expr->GetExprType() == node::kExprAll) {
            for (size_t i = 0; i < expr->GetChildNum(); ++i) {
                auto project_under_asterisk = dynamic_cast<node::ColumnRefNode*>(expr->GetChild(i));
                CHECK_TRUE(project_under_asterisk != nullptr, kPlanError);
                output->Add(project_under_asterisk->GetColumnName(), project_under_asterisk, nullptr);
            }
        } else {
            output->Add(pp_node->GetName(), expr, pp_node->frame());
        }
    }
    output->SetPrimaryFrame(primary_frame);
    return Status::OK();
}

Status BatchModeTransformer::InstantiateLLVMFunction(const FnInfo* fn_info) {
    CHECK_TRUE(fn_info->IsValid(), kCodegenError, "Fail to install llvm function, function info is invalid");
    codegen::CodeGenContext codegen_ctx(module_, fn_info->schemas_ctx(), plan_ctx_.parameter_types(), node_manager_);
    codegen::RowFnLetIRBuilder builder(&codegen_ctx);
    return builder.Build(fn_info->fn_name(), fn_info->fn_def(), fn_info->GetPrimaryFrame(), fn_info->GetFrames(),
                         *fn_info->fn_schema());
}

bool BatchModeTransformer::AddDefaultPasses() {
    AddPass(PhysicalPlanPassType::kPassSimpleProjectsOptimized);
    AddPass(PhysicalPlanPassType::kPassFilterOptimized);
    AddPass(PhysicalPlanPassType::kPassLeftJoinOptimized);
    AddPass(PhysicalPlanPassType::kPassGroupAndSortOptimized);
    AddPass(PhysicalPlanPassType::kPassLimitOptimized);
    AddPass(PhysicalPlanPassType::kPassClusterOptimized);
    return false;
}
Status BatchModeTransformer::ValidateOnlyFullGroupBy(const node::ProjectListNode* project_list,
                                                     const node::ExprListNode* group_keys,
                                                     const SchemasContext* schemas_ctx) {
    CHECK_TRUE(project_list != nullptr && schemas_ctx != nullptr, common::kNullInputPointer,
               "input parameters are null");

    const node::ExprNode* having_condition = project_list->GetHavingCondition();
    // query without GROUP BY
    if (node::ExprListNullOrEmpty(group_keys)) {
        // ONLY_FULL_GROUP_BY Rule #1: OpenMLDB only support ONLY_FULL_GROUP_BY mode
        // In aggregated query without GROUP BY, expression #%u of %s contains nonaggregated column '%s';
        // this is incompatible with sql_mode=only_full_group_by
        // -- unacceptable sql
        // SELECT col1, col2, SUM(col3) from t1;
        if (project_list->HasAggProject() && project_list->HasRowProject()) {
            for (size_t idx = 0; idx < project_list->GetProjects().size(); idx++) {
                node::ProjectNode* project = dynamic_cast<node::ProjectNode*>(project_list->GetProjects()[idx]);
                CHECK_TRUE(project->IsAgg(), common::kPlanError,
                           "In aggregated query without GROUP BY, expression #", project->GetPos(),
                           " of SELECT list contains nonaggregated project '",
                           project->GetExpression()->GetExprString(),
                           "'; this is incompatible with sql_mode=only_full_group_by")
            }
        }
        // HAVING
        if (nullptr != having_condition) {
            // ONLY_FULL_GROUP_BY Rule #2: Having clause can only be used in conjunction with aggregated query
            CHECK_TRUE(project_list->HasAggProject(), common::kPlanError,
                       "Having clause can only be used in conjunction with aggregated query")
            // ONLY_FULL_GROUP_BY Rule #3: OpenMLDB only support ONLY_FULL_GROUP_BY mode
            // In aggregated query without GROUP BY, Having clause contains nonaggregated column '%s';
            // this is incompatible with sql_mode=only_full_group_by
            CHECK_TRUE(node::IsAggregationExpression(library_, having_condition), common::kPlanError,
                       "In aggregated query without GROUP BY, "
                       "Having clause contains nonaggregated project '",
                       having_condition->GetExprString(), "'; this is incompatible with sql_mode=only_full_group_by")
        }
    } else {
        // query with GROUP BY
        std::set<size_t> group_column_ids;
        CHECK_STATUS(schemas_ctx->ResolveExprDependentColumns(group_keys, &group_column_ids));
        // ONLY_FULL_GROUP_BY Rule #4: OpenMLDB only support ONLY_FULL_GROUP_BY mode
        // Expression #%u of %s is not in GROUP BY clause and contains nonaggregated column '%s'
        // which is not functionally dependent on columns in GROUP BY clause;
        // this is incompatible with sql_mode=only_full_group_by
        // e.g.
        // -- invalid with ONLY_FULL_GROUP_BY
        // ```
        // SELECT name, address, MAX(age) FROM t GROUP BY name;
        if (project_list->HasRowProject()) {
            for (size_t idx = 0; idx < project_list->GetProjects().size(); idx++) {
                node::ProjectNode* project = dynamic_cast<node::ProjectNode*>(project_list->GetProjects()[idx]);
                if (!project->IsAgg()) {
                    std::set<size_t> depend_column_ids;
                    CHECK_STATUS(
                        schemas_ctx->ResolveExprDependentColumns(project->GetExpression(), &depend_column_ids));
                    for (auto& column_id : depend_column_ids) {
                        if (group_column_ids.count(column_id) == 0) {
                            std::string db;
                            std::string table;
                            std::string column;
                            CHECK_STATUS(schemas_ctx->ResolveDbTableColumnByID(column_id, &db, &table, &column));
                            std::string column_path = db + "." + table + "." + column;
                            FAIL_STATUS(common::kPlanError, "Expression #", project->GetPos(),
                                        " of SELECT list is not in GROUP BY clause and contains nonaggregated column '",
                                        column_path,
                                        "' which is not functionally dependent on columns in GROUP BY clause; this is "
                                        "incompatible with sql_mode=only_full_group_by")
                        }
                    }
                }
            }
        }
        // GROUP BY ... HAVING ...
        if (nullptr != having_condition) {
            // ONLY_FULL_GROUP_BY Rule #5:
            // Expression #%u of %s is not in GROUP BY clause and contains nonaggregated column '%s'
            // which is not functionally dependent on columns in GROUP BY clause;
            // this is incompatible with sql_mode=only_full_group_by
            if (!node::IsAggregationExpression(library_, having_condition)) {
                std::set<size_t> depend_column_ids;
                CHECK_STATUS(schemas_ctx->ResolveExprDependentColumns(having_condition, &depend_column_ids))

                for (auto column_id : depend_column_ids) {
                    if (group_column_ids.count(column_id) == 0) {
                        std::string db;
                        std::string table;
                        std::string column;
                        CHECK_STATUS(schemas_ctx->ResolveDbTableColumnByID(column_id, &db, &table, &column));
                        std::string column_path = db + "." + table + "." + column;
                        FAIL_STATUS(common::kPlanError,
                                    "Having clause is not in GROUP BY clause and contains nonaggregated column '",
                                    column_path,
                                    "' which is not functionally dependent on columns in GROUP BY clause; this is "
                                    "incompatible with sql_mode=only_full_group_by")
                    }
                }
            }
        }
    }
    return base::Status::OK();
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

    ColumnProjects const_projects;
    CHECK_STATUS(ExtractProjectInfos(projects, nullptr, &const_projects));

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
    const node::ExprNode* having_condition = project_list->GetHavingCondition();
    const node::ExprListNode* group_keys = nullptr;
    if (kGroupAggregation == project_type) {
        CHECK_STATUS(ExtractGroupKeys(depend, &group_keys));
    }
    if (nullptr == project_list->GetW()) {
        CHECK_STATUS(ValidateOnlyFullGroupBy(project_list, group_keys, depend->schemas_ctx()));
    }
    // extract window frame if any
    const node::FrameNode* primary_frame = nullptr;
    if (project_list->GetW() != nullptr) {
        primary_frame = project_list->GetW()->frame_node();
    }

    bool has_all_project = false;

    for (auto iter = projects.cbegin(); iter != projects.cend(); iter++) {
        auto project_node = dynamic_cast<node::ProjectNode*>(*iter);
        auto expr = project_node->GetExpression();
        CHECK_TRUE(expr != nullptr, kPlanError, "Invalid project: expression is null");
        if (node::kExprAll == expr->expr_type_) {
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
    CHECK_STATUS(ExtractProjectInfos(projects, primary_frame, &column_projects));

    if (append_input) {
        CHECK_TRUE(project_type == kWindowAggregation, kPlanError,
                   "Only window agg allow append input");
    }

    // Create physical project op
    switch (project_type) {
        case kRowProject: {
            CHECK_TRUE(having_condition == nullptr, kPlanError,
                       "Can't support having clause and row project simultaneously")
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
            CHECK_TRUE(having_condition == nullptr, kPlanError,
                       "Can't support having clause and table project simultaneously")
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
            PhysicalAggregationNode* agg_op = nullptr;
            CHECK_STATUS(CreateOp<PhysicalAggregationNode>(&agg_op, depend,
                                                           column_projects, having_condition));
            *output = agg_op;
            break;
        }
        case kGroupAggregation: {
            CHECK_TRUE(!node::ExprListNullOrEmpty(group_keys), kPlanError,
                       "Can not create group agg with non group keys");

            PhysicalGroupAggrerationNode* agg_op = nullptr;
            CHECK_STATUS(CreateOp<PhysicalGroupAggrerationNode>(
                &agg_op, depend, column_projects, having_condition, group_keys));
            *output = agg_op;
            break;
        }
        case kWindowAggregation: {
            PhysicalWindowAggrerationNode* window_agg_op = nullptr;
            // window clause and having clause cannot exist simultaneously
            CHECK_TRUE(nullptr == having_condition, kPlanError,
                       "Can't support having clause and window clause simultaneously")
            CHECK_STATUS(CreateOp<PhysicalWindowAggrerationNode>(
                &window_agg_op, depend, column_projects, WindowOp(project_list->GetW()),
                project_list->GetW()->instance_not_in_window(), append_input,
                project_list->GetW()->exclude_current_time()));

            if (!project_list->GetW()->union_tables().empty()) {
                for (auto iter = project_list->GetW()->union_tables().cbegin();
                     iter != project_list->GetW()->union_tables().cend(); iter++) {
                    PhysicalOpNode* union_table_op;
                    CHECK_STATUS(TransformPlanOp(*iter, &union_table_op));
                    PhysicalRenameNode* rename_union_op = nullptr;
                    CHECK_STATUS(CreateOp<PhysicalRenameNode>(&rename_union_op, union_table_op,
                                                              depend->schemas_ctx()->GetName()));
                    CHECK_TRUE(window_agg_op->AddWindowUnion(rename_union_op), kPlanError,
                               "Fail to add window union table");
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
Status BatchModeTransformer::TransformProjectOp(node::ProjectListNode* project_list,
                                                PhysicalOpNode* node,
                                                bool append_input,
                                                PhysicalOpNode** output) {
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
                        CheckWindow(project_list->GetW(), depend->schemas_ctx()));
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
            case PhysicalPlanPassType::kPassSimpleProjectsOptimized: {
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
                if (catalog_->IndexSupport()) {
                    GroupAndSortOptimized pass(&plan_ctx_);
                    transformed = pass.Apply(cur_op, &new_op);
                }
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
            case PhysicalPlanPassType::kPassSplitAggregationOptimized: {
                SplitAggregationOptimized split_pass(&plan_ctx_);
                transformed = split_pass.Apply(cur_op, &new_op);
                break;
            }
            case PhysicalPlanPassType::kPassLongWindowOptimized: {
                LongWindowOptimized pass(&plan_ctx_);
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

    if (enable_batch_window_column_pruning_) {
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

// 1. validate plan is currently supported
//    - the right key used for partition, whose type should be one of: [bool, intxx, string, date, timestamp]
// 2. validate if plan is optimized for performance_sensitive
//    - query condition hit to a table index
//    - not aggregation over table
Status BatchModeTransformer::ValidatePlan(PhysicalOpNode* node) {
    CHECK_TRUE(nullptr != node, kPlanError, "Invalid physical node: null");
    // TODO(liguo): create a `visitor` method for nodes, single dfs applied
    CHECK_STATUS(ValidatePlanSupported(node));
    return Status::OK();
}

Status BatchModeTransformer::ValidatePlanSupported(const PhysicalOpNode* in) {
    for (const auto child : in->GetProducers()) {
        CHECK_STATUS(ValidatePlanSupported(child));
    }
    switch (in->GetOpType()) {
        case kPhysicalOpJoin: {
            const auto& join_op = dynamic_cast<const PhysicalJoinNode*>(in);
            switch (join_op->join_.join_type()) {
                case node::kJoinTypeLast:
                case node::kJoinTypeLeft: {
                    CHECK_STATUS(CheckPartitionColumn(join_op->join().right_key().keys(), join_op->schemas_ctx()));
                    break;
                }
                default: {
                    break;
                }
            }
            break;
        }
        case kPhysicalOpRequestJoin: {
            const auto& join_op = dynamic_cast<const PhysicalRequestJoinNode*>(in);
            switch (join_op->join_.join_type()) {
                case node::kJoinTypeLast:
                case node::kJoinTypeLeft: {
                    CHECK_STATUS(CheckPartitionColumn(join_op->join().right_key().keys(), join_op->schemas_ctx()));
                    break;
                }
                default: {
                    break;
                }
            }
            break;
        }
        case kPhysicalOpGroupBy: {
            const auto& group_op = dynamic_cast<const PhysicalGroupNode*>(in);
            CHECK_STATUS(CheckPartitionColumn(group_op->group().keys(), group_op->schemas_ctx()));
            break;
        }
        case kPhysicalOpProject: {
            const auto& project_op = dynamic_cast<const PhysicalProjectNode*>(in);

            switch (project_op->project_type_) {
                case kWindowAggregation: {
                    const auto& win_agg_op = dynamic_cast<const PhysicalWindowAggrerationNode*>(project_op);

                    CHECK_STATUS(CheckPartitionColumn(win_agg_op->window_.partition().keys(),
                                                      win_agg_op->GetProducer(0)->schemas_ctx()));

                    // union table's partition key not check
                    // physical plan generation will fail because of schema not match

                    for (const auto& join : win_agg_op->window_joins_.window_joins_) {
                        CHECK_STATUS(CheckPartitionColumn(join.second.right_key().keys(), join.first->schemas_ctx()));
                    }
                    break;
                }
                default: {
                    break;
                }
            }

            break;
        }
        case kPhysicalOpRequestUnion: {
            const auto& union_op = dynamic_cast<const PhysicalRequestUnionNode*>(in);
            CHECK_STATUS(CheckPartitionColumn(union_op->window().partition().keys(), union_op->schemas_ctx()));
            break;
        }
        default: {
            break;
        }
    }
    return Status::OK();
}

// Override validate plan
// Validate plan with basic rules defined for general batch mode SQL
// Validate plan with specific rules designed for performance_sensitive:
//  - ValidateRequestTable
//  - ValidateIndexOptimization
//  - ValidateNotAggregationOverTable
Status RequestModeTransformer::ValidatePlan(PhysicalOpNode* node) {
    CHECK_STATUS(BatchModeTransformer::ValidatePlan(node))
    PhysicalOpNode* primary_source = nullptr;

    // OnlineServing restriction: Expect to infer one and only one request table from given SQL
    CHECK_STATUS(ValidateRequestTable(node, &primary_source), "Fail to validate physical plan")

    // For Online serving, SQL queries should be designed extremely performance-sensitive to satisfy the real-time
    // requirements. Thus, we need to Validate if the SQL has been optimized well enough
    if (performance_sensitive_) {
        CHECK_STATUS(ValidateIndexOptimization(node), "Fail to support physical plan in performance sensitive mode");
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

Status BatchModeTransformer::TransformPhysicalPlan(const ::hybridse::node::PlanNodeList& trees,
                                                   ::hybridse::vm::PhysicalOpNode** output) {
    CHECK_TRUE(module_ != nullptr && !trees.empty(), kPlanError, "Module or logical trees is empty");
    CHECK_TRUE(output != nullptr, kPlanError, "output is nullptr")
    auto it = trees.begin();
    for (; it != trees.end(); ++it) {
        const ::hybridse::node::PlanNode* node = *it;
        switch (node->GetType()) {
            case ::hybridse::node::kPlanTypeFuncDef: {
                auto func_def_plan =
                    dynamic_cast<const ::hybridse::node::FuncDefPlanNode*>(node);
                CHECK_STATUS(GenFnDef(func_def_plan), "Fail to compile user function def");
                *output = nullptr;
                break;
            }
            case ::hybridse::node::kPlanTypeUnion: {
                FAIL_STATUS(kPlanError, "Non-support UNION OP");
                break;
            }
            case node::kPlanTypeCreate: {
                CHECK_STATUS(TransformCreateTableOp(dynamic_cast<const node::CreatePlanNode*>(node), output),
                    "Fail to transform create table op");
                break;
            }
            case ::hybridse::node::kPlanTypeQuery: {
                PhysicalOpNode* physical_plan = nullptr;
                CHECK_STATUS(
                    TransformQueryPlan(dynamic_cast<const ::hybridse::node::QueryPlanNode*>(node), &physical_plan),
                    "Fail to transform query statement");

                DLOG(INFO) << "Before optimization: \n" << physical_plan->GetTreeString();

                PhysicalOpNode* optimized_physical_plan = nullptr;
                ApplyPasses(physical_plan, &optimized_physical_plan);

                DLOG(INFO) << "After optimization: \n" << optimized_physical_plan->GetTreeString();
                CHECK_STATUS(ValidatePlan(optimized_physical_plan));
                std::set<PhysicalOpNode*> node_visited_dict;
                CHECK_STATUS(InitFnInfo(optimized_physical_plan, &node_visited_dict),
                             "Fail to generate functions for physical plan");
                *output = optimized_physical_plan;
                break;
            }
            case ::hybridse::node::kPlanTypeCreateSp: {
                auto sp_plan =
                    dynamic_cast<const ::hybridse::node::CreateProcedurePlanNode*>(node);
                return TransformPhysicalPlan(sp_plan->GetInnerPlanNodeList(), output);
            }
            case ::hybridse::node::kPlanTypeSelectInto: {
                auto select_into_plan = dynamic_cast<const ::hybridse::node::SelectIntoPlanNode*>(node);
                // should do optimization
                ::hybridse::vm::PhysicalOpNode* child = nullptr;
                CHECK_STATUS(TransformPhysicalPlan({select_into_plan->Query()}, &child))
                return TransformSelectIntoOp(select_into_plan, child, output);
            }
            case ::hybridse::node::kPlanTypeLoadData:
            case ::hybridse::node::kPlanTypeDelete: {
                return TransformPlanOp(node, output);
            }
            case ::hybridse::node::kPlanTypeInsert: {
                auto* insert_plan_node = dynamic_cast<const ::hybridse::node::InsertPlanNode*>(node);
                PhysicalInsertNode* insert_node = nullptr;
                CHECK_STATUS(CreateOp(&insert_node, insert_plan_node->GetInsertNode()));
                *output = insert_node;
                return Status::OK();
            }
            default: {
                return {kPlanError, "Plan type not supported: " + node::NameOfPlanNodeType(node->GetType())};
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
    builder.Build(fn_plan->fn_def_, &fn, status);
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
    if (nullptr != filter->condition()) {
        node::ExprListNode expr_list;
        expr_list.AddChild(const_cast<node::ExprNode*>(filter->condition()));
        CHECK_STATUS(plan_ctx_.InitFnDef(&expr_list, schemas_ctx, true, filter))
    }
    return Status::OK();
}
Status BatchModeTransformer::GenHavingFilter(
    ConditionFilter* filter, const SchemasContext* schemas_ctx) {
    if (nullptr != filter->condition()) {
        node::ExprListNode expr_list;
        expr_list.AddChild(const_cast<node::ExprNode*>(filter->condition()));
        CHECK_STATUS(plan_ctx_.InitFnDef(&expr_list, schemas_ctx, false, filter))
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
        for (uint32_t i = 0; i < sort->orders_->order_expressions_->GetChildNum(); i++) {
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

/**
* check partition keys is acceptable type, which should be one of
*   bool, intXX, string, date, timestamp
*/
Status BatchModeTransformer::CheckPartitionColumn(const node::ExprListNode* partition, const SchemasContext* ctx) {
    if (partition == nullptr) {
        return Status::OK();
    }
    for (uint32_t i = 0; i < partition->GetChildNum(); ++i) {
        const auto child = partition->GetChild(i);
        switch (child->GetExprType()) {
            case node::kExprColumnRef: {
                auto column = dynamic_cast<const node::ColumnRefNode*>(child);
                size_t schema_idx = 0;
                size_t col_idx = 0;
                if (ctx->ResolveColumnRefIndex(column, &schema_idx, &col_idx).isOK()) {
                    auto type = ctx->GetSchemaSource(schema_idx)->GetColumnType(col_idx);
                    switch (type) {
                        case type::kBool:
                        case type::kInt16:
                        case type::kInt32:
                        case type::kInt64:
                        case type::kVarchar:
                        case type::kDate:
                        case type::kTimestamp:
                            break;
                        default: {
                            FAIL_STATUS(common::kPhysicalPlanError, "unsupported partition key: '",
                                        column->GetExprString(), "', type is ", node::TypeName(type),
                                        ", should be bool, intxx, string, date or timestamp");
                        }
                    }
                }
            }
            default: {
                break;
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

    CHECK_STATUS(CheckTimeOrIntegerOrderColumn(w_ptr->GetOrders(), schemas_ctx));

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

Status BatchModeTransformer::ValidateRequestJoinIndexOptimization(const Join& join, PhysicalOpNode* in) {
    return Status();
}

RequestModeTransformer::RequestModeTransformer(node::NodeManager* node_manager, const std::string& db,
                                               const std::shared_ptr<Catalog>& catalog,
                                               const codec::Schema* parameter_types, ::llvm::Module* module,
                                               udf::UdfLibrary* library, const std::set<size_t>& common_column_indices,
                                               const bool cluster_optimized, const bool enable_batch_request_opt,
                                               bool enable_expr_opt, bool performance_sensitive,
                                               const std::unordered_map<std::string, std::string>* options)
    : BatchModeTransformer(node_manager, db, catalog, parameter_types, module, library, cluster_optimized,
                           enable_expr_opt, true, false, options),
      enable_batch_request_opt_(enable_batch_request_opt), performance_sensitive_(performance_sensitive) {
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

    CHECK_STATUS(CompleteProjectList(node, depend));

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
            project_list->AddProject(
                node_manager_->MakeRowProjectNode(pos, "*", all_expr));
        } else {
            project_list->AddProject(node_manager_->MakeRowProjectNode(
                pos, project_node->GetName(), node_manager_->MakeColumnRefNode(project_node->GetName(), "")));
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

    CHECK_STATUS(CheckTimeOrIntegerOrderColumn(node->orders_, request_join_op->schemas_ctx()));

    *output = request_join_op;
    return Status::OK();
}

Status RequestModeTransformer::TransformScanOp(const node::TablePlanNode* node,
                                               PhysicalOpNode** output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError,
               "Input node or output node is null");

    if (node->db_.empty()) {
        auto res = ResolveCTERef(node->table_, node->IsPrimary());
        if (res.ok()) {
            *output = res.value();
            return Status::OK();
        }

        if (!absl::IsNotFound(res.status())) {
            FAIL_STATUS(common::kPlanError, res.status().ToString());
        }
    }

    if (node->IsPrimary()) {
        auto table = catalog_->GetTable(node->db_.empty() ? db_ : node->db_, node->table_);
        CHECK_TRUE(table != nullptr, common::kTableNotFound, "Fail to transform data_provider op: table ",
                   (node->db_.empty() ? db_ : node->db_), ".", node->table_, " not exist!");

        PhysicalRequestProviderNode* provider = nullptr;
        CHECK_STATUS(CreateOp<PhysicalRequestProviderNode>(&provider, table));
        *output = provider;
        request_schema_ = *(*output)->GetOutputSchema();
        request_name_ = table->GetName();
        request_db_name_ = table->GetDatabase();
        return Status::OK();
    } else {
        return BatchModeTransformer::TransformScanOp(node, output);
    }
}
Status RequestModeTransformer::ValidateRequestTable(
    PhysicalOpNode* in, PhysicalOpNode** request_table) {
    CHECK_TRUE(in != NULL, kPlanError, "NULL Physical Node");

    switch (in->GetOpType()) {
        case vm::kPhysicalOpDataProvider: {
            CHECK_TRUE(kProviderTypeRequest == dynamic_cast<PhysicalDataProviderNode*>(in)->provider_type_, kPlanError,
                       "Expect a request table but a ",
                       vm::DataProviderTypeName(dynamic_cast<PhysicalDataProviderNode*>(in)->provider_type_),
                       in->GetTreeString())
            *request_table = in;
            return Status::OK();
        }
        case vm::kPhysicalOpJoin:
        case vm::kPhysicalOpUnion:
        case vm::kPhysicalOpPostRequestUnion:
        case vm::kPhysicalOpRequestUnion:
        case vm::kPhysicalOpRequestAggUnion:
        case vm::kPhysicalOpRequestJoin: {
            vm::PhysicalOpNode* left_primary_source = nullptr;
            CHECK_STATUS(ValidateRequestTable(in->GetProducer(0), &left_primary_source))
            CHECK_TRUE(
                nullptr != left_primary_source, kPlanError,
                "Fail to infer a request table")
            // Case 1:
            // binary_node
            //    + Left - PrimarySource
            //    + Right - TableProvider
            if (isSourceFromTableOrPartition(in->GetProducer(1))) {
                *request_table = left_primary_source;
                return Status::OK();
            }

            vm::PhysicalOpNode* right_primary_source = nullptr;
            CHECK_STATUS(ValidateRequestTable(in->GetProducer(1), &right_primary_source))
            CHECK_TRUE(
                nullptr != right_primary_source, kPlanError,
                "Fail to infer a request table")
            // Case 2:
            // binary_node
            //      + Left <-- SamePrimarySource1
            //      + Right <-- SamePrimarySource1
            CHECK_TRUE(left_primary_source->Equals(right_primary_source), kPlanError,
                       "left path and right path has different request table")
            *request_table = left_primary_source;
            return Status::OK();
        }
        case vm::kPhysicalOpConstProject: {
            FAIL_STATUS(kPlanError,
                       "Non-support Const Project in request mode", in->GetTreeString());
            break;
        }
        default: {
            CHECK_TRUE(in->GetProducerCnt() == 1, kPlanError,
                       "Non-support Op ", PhysicalOpTypeName(in->GetOpType()))
            CHECK_STATUS(ValidateRequestTable(in->GetProducer(0), request_table));
            return Status::OK();
        }
    }
    return Status::OK();
}

// transform a single `ProjectListNode` of `ProjectPlanNode`
Status RequestModeTransformer::TransformProjectOp(
    node::ProjectListNode* project_list, PhysicalOpNode* depend,
    bool append_input, PhysicalOpNode** output) {
    PhysicalOpNode* new_depend = depend;
    if (nullptr != project_list->GetW()) {
        CHECK_STATUS(TransformWindowOp(depend, project_list->GetW(), &new_depend));
    }
    switch (new_depend->GetOutputType()) {
        case kSchemaTypeRow:
            CHECK_TRUE(!project_list->HasAggProject(), kPlanError, "Non-support aggregation project on request row")
            return CreatePhysicalProjectNode(
                kRowProject, new_depend, project_list, append_input, output);
        case kSchemaTypeGroup:
            return CreatePhysicalProjectNode(kGroupAggregation, new_depend,
                                             project_list, append_input,
                                             output);
        case kSchemaTypeTable:
            if (project_list->HasAggProject()) {
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

Status RequestModeTransformer::TransformLoadDataOp(const node::LoadDataPlanNode* node, PhysicalOpNode** output) {
    FAIL_STATUS(common::kPlanError, "Non-support LoadData in request mode");
}

Status BatchModeTransformer::TransformWithClauseEntry(const node::WithClauseEntryPlanNode* node, PhysicalOpNode** out) {
    CHECK_TRUE(node != nullptr, common::kNullInputPointer);

    PhysicalOpNode* transformed = nullptr;
    CHECK_STATUS(TransformPlanOp(node->query_, &transformed));

    PhysicalRenameNode* rename = nullptr;
    CHECK_STATUS(CreateOp(&rename, transformed, node->alias_));
    *out = rename;
    return Status::OK();
}

void BatchModeTransformer::PushCTEEnv(const internal::CTEEnv& env) {
    closure_ = node_manager_->MakeObj<internal::Closure>(closure_, env);
}

void BatchModeTransformer::ReplaceClosure(internal::Closure* clu) {
    closure_ = clu;
}

Status BatchModeTransformer::PopCTEs() {
    CHECK_TRUE(closure_ != nullptr, common::kPlanError, "closure is null");
    closure_ = closure_->parent;
    return Status::OK();
}

absl::StatusOr<PhysicalOpNode*> BatchModeTransformer::ResolveCTERef(absl::string_view tb_name, bool is_primary_path) {
    return ResolveCTERefImpl(tb_name, false, false);
}

absl::StatusOr<PhysicalOpNode*> BatchModeTransformer::ResolveCTERefImpl(absl::string_view tb_name, bool request_mode,
                                                                        bool is_primary_path) {
    auto* cur_closure = closure_;
    auto it = cur_closure->cte_map.find(tb_name);
    if (it != cur_closure->cte_map.end()) {
        auto* entry = it->second.top();

        if (entry->transformed_op == nullptr) {
            if (request_mode) {
                // - for a primary table plan node refer to a WITH clause entry, query inside WITH clause entry
                //   must have a request table
                // - for a non-primary table plan node refer to a WITH clause entry, query inside WITH clause
                //   will have a request table only if it is not a simple reference to a physical table. Whether
                //   the extracted request table is the same as outside WITH clause, is checked later in
                //   `RequestModeTransformer::ValidatePlan`, and hence is not necessary to verify here.
                //   see more in `Planner::IsTable`
                auto s = ::hybridse::plan::Planner::ValidPlanForRequestMode(entry->node->query_, is_primary_path);
                if (!s.isOK()) {
                    return absl::InternalError(s.str());
                }
            }

            {
                // transform physical node for with clause only when actually need it
                PhysicalOpNode* transformed = nullptr;
                // update closure for the WithClauseEntry query transformation
                auto* old_closure = closure_;
                ReplaceClosure(entry->closure);
                absl::Cleanup restore_clu = [this, old_closure]() { ReplaceClosure(old_closure); };

                auto s = TransformPlanOp(entry->node, &transformed);
                if (s.isOK()) {
                    entry->transformed_op = transformed;
                    return transformed;
                }

                return absl::InternalError(s.str());
            }

        } else {
            return entry->transformed_op;
        }
    }

    return absl::NotFoundError(absl::StrCat(tb_name, " not found"));
}

absl::StatusOr<PhysicalOpNode*> RequestModeTransformer::ResolveCTERef(absl::string_view tb_name, bool is_primary_path) {
    return ResolveCTERefImpl(tb_name, true, is_primary_path);
}

}  // namespace vm
}  // namespace hybridse
