/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * transform.cc
 *
 * Author: chenjing
 * Date: 2020/3/13
 *--------------------------------------------------------------------------
 **/
#include "vm/transform.h"
#include <map>
#include <set>
#include <stack>
#include <unordered_map>
#include "codegen/context.h"
#include "codegen/fn_ir_builder.h"
#include "codegen/fn_let_ir_builder.h"
#include "vm/physical_op.h"
#include "vm/schemas_context.h"

#include "passes/expression/expr_pass.h"
#include "passes/lambdafy_projects.h"
#include "passes/physical/batch_request_optimize.h"
#include "passes/resolve_fn_and_attrs.h"

using ::fesql::base::Status;
using ::fesql::common::kPlanError;

namespace fesql {
namespace vm {

std::ostream& operator<<(std::ostream& output,
                         const fesql::vm::LogicalOp& thiz) {
    return output << *(thiz.node_);
}
bool TransformLogicalTreeToLogicalGraph(
    const ::fesql::node::PlanNode* node, LogicalGraph* graph_ptr,
    fesql::base::Status& status) {  // NOLINT

    if (nullptr == node || nullptr == graph_ptr) {
        status.msg = "node or graph_ptr is null";
        status.code = common::kOpGenError;
        LOG(WARNING) << status;
        return false;
    }
    auto& graph = *graph_ptr;
    std::stack<LogicalOp> stacks;
    LogicalOp op(node);
    graph.AddVertex(op);
    stacks.push(op);
    while (!stacks.empty()) {
        auto source = stacks.top();
        stacks.pop();
        auto& children = source.node_->GetChildren();
        if (!children.empty()) {
            for (auto iter = children.cbegin(); iter != children.cend();
                 iter++) {
                LogicalOp target(*iter);
                if (!graph.IsExist(target)) {
                    stacks.push(target);
                }
                graph.AddEdge(source, target);
            }
        }
    }
    return true;
}

BatchModeTransformer::BatchModeTransformer(
    node::NodeManager* node_manager, const std::string& db,
    const std::shared_ptr<Catalog>& catalog, ::llvm::Module* module,
    const udf::UDFLibrary* library)
    : node_manager_(node_manager),
      db_(db),
      catalog_(catalog),
      module_(module),
      id_(0),
      performance_sensitive_mode_(false),
      cluster_optimized_mode_(false),
      library_(library),
      plan_ctx_(node_manager, library, db, catalog, false) {}

BatchModeTransformer::BatchModeTransformer(
    node::NodeManager* node_manager, const std::string& db,
    const std::shared_ptr<Catalog>& catalog, ::llvm::Module* module,
    const udf::UDFLibrary* library, bool performance_sensitive,
    bool cluster_optimized_mode, bool enable_expr_opt)
    : node_manager_(node_manager),
      db_(db),
      catalog_(catalog),
      module_(module),
      id_(0),
      performance_sensitive_mode_(performance_sensitive),
      cluster_optimized_mode_(cluster_optimized_mode),
      library_(library),
      plan_ctx_(node_manager, library, db, catalog, enable_expr_opt) {}

BatchModeTransformer::~BatchModeTransformer() {}

Status BatchModeTransformer::TransformPlanOp(const node::PlanNode* node,
                                             PhysicalOpNode** output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kPlanError,
               "Input node or output node is null");

    LogicalOp logical_op = LogicalOp(node);
    auto map_iter = op_map_.find(logical_op);
    // logical plan node already exist
    if (map_iter != op_map_.cend()) {
        *output = map_iter->second;
        return Status::OK();
    }

    Status status;
    ::fesql::vm::PhysicalOpNode* op = nullptr;
    switch (node->type_) {
        case node::kPlanTypeLimit: {
            status = TransformLimitOp(
                dynamic_cast<const ::fesql::node::LimitPlanNode*>(node), &op);
            break;
        }
        case node::kPlanTypeProject: {
            status = TransformProjectPlanOp(
                dynamic_cast<const ::fesql::node::ProjectPlanNode*>(node), &op);
            break;
        }
        case node::kPlanTypeJoin: {
            status = TransformJoinOp(
                dynamic_cast<const ::fesql::node::JoinPlanNode*>(node), &op);
            break;
        }
        case node::kPlanTypeUnion: {
            status = TransformUnionOp(
                dynamic_cast<const ::fesql::node::UnionPlanNode*>(node), &op);
            break;
        }
        case node::kPlanTypeGroup: {
            status = TransformGroupOp(
                dynamic_cast<const ::fesql::node::GroupPlanNode*>(node), &op);
            break;
        }
        case node::kPlanTypeSort: {
            status = TransformSortOp(
                dynamic_cast<const ::fesql::node::SortPlanNode*>(node), &op);
            break;
        }
        case node::kPlanTypeFilter: {
            status = TransformFilterOp(
                dynamic_cast<const ::fesql::node::FilterPlanNode*>(node), &op);
            break;
        }
        case node::kPlanTypeTable: {
            status = TransformScanOp(
                dynamic_cast<const ::fesql::node::TablePlanNode*>(node), &op);
            break;
        }
        case node::kPlanTypeQuery: {
            status = TransformQueryPlan(
                dynamic_cast<const ::fesql::node::QueryPlanNode*>(node), &op);
            break;
        }
        case node::kPlanTypeRename: {
            status = TransformRenameOp(
                dynamic_cast<const ::fesql::node::RenamePlanNode*>(node), &op);
            break;
        }
        case node::kPlanTypeDistinct: {
            status = TransformDistinctOp(
                dynamic_cast<const ::fesql::node::DistinctPlanNode*>(node),
                &op);
            break;
        }
        default: {
            return Status(kPlanError,
                          "Fail to transform physical plan: "
                          "can't handle type " +
                              node::NameOfPlanNodeType(node->type_));
        }
    }
    CHECK_STATUS(status, "Fail to tranform physical plan: fail at:\n" +
                             node->GetTreeString());
    op_map_[logical_op] = op;
    *output = op;
    return status;
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

static bool ResetProducer(PhysicalPlanContext* plan_ctx, PhysicalOpNode* op,
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

    PhysicalOpNode* depend = nullptr;
    if (!node->GetChildren().empty() && nullptr != node->GetChildren()[0]) {
        CHECK_STATUS(TransformPlanOp(node->GetChildren()[0], &depend));
    }
    CHECK_TRUE(!node->project_list_vec_.empty(), kPlanError,
               "Fail transform project op: empty projects");
    if (1 == node->project_list_vec_.size()) {
        return TransformProjectOp(dynamic_cast<fesql::node::ProjectListNode*>(
                                      node->project_list_vec_[0]),
                                  depend, false, output);
    }

    // 处理project_list_vec_[1...N-1], 串联执行windowAggWithAppendInput
    std::vector<PhysicalOpNode*> ops;
    int32_t project_cnt = 0;
    for (size_t i = node->project_list_vec_.size() - 1; i > 0; i--) {
        fesql::node::ProjectListNode* project_list =
            dynamic_cast<fesql::node::ProjectListNode*>(
                node->project_list_vec_[i]);
        project_cnt++;
        PhysicalOpNode* project_op = nullptr;
        CHECK_STATUS(
            TransformProjectOp(project_list, depend, true, &project_op));
        depend = project_op;
    }

    // 第一个Project节点除了计算投影表达式之外，还需要筛选出前面窗口的表达式结果
    // TODO(chenjing): 这部分代码可读性还是太差
    fesql::node::ProjectListNode* first_project_list =
        dynamic_cast<fesql::node::ProjectListNode*>(node->project_list_vec_[0]);
    auto project_list = node_manager_->MakeProjectListPlanNode(
        first_project_list->w_ptr_, first_project_list->is_window_agg_);
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

/**
 * Check whether expression only depend on node's `child_idx`th input.
 */
static Status CheckExprDependOnChildOnly(
    const node::ExprNode* expr, const SchemasContext* child_schemas_ctx) {
    std::set<size_t> column_ids;
    return child_schemas_ctx->ResolveExprDependentColumns(expr, &column_ids);
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
                        !node::ExprListNullOrEmpty(orders->order_by_)) {
                        CHECK_STATUS(CheckExprDependOnChildOnly(
                                         orders->order_by_, child_schemas_ctx),
                                     "Fail to handle window: group "
                                     "expression should belong to left table");
                    }

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

    PhysicalSortNode* sort_op = nullptr;
    CHECK_STATUS(CreateOp<PhysicalSortNode>(&sort_op, left, node->order_list_));
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
    const ::fesql::node::PlanNode* node, ::fesql::vm::PhysicalOpNode** output) {
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
            auto all_expr = dynamic_cast<node::AllNode*>(expr);
            for (size_t slice = 0; slice < schemas_ctx->GetSchemaSourceSize();
                 ++slice) {
                auto schema_source = schemas_ctx->GetSchemaSource(slice);
                for (size_t k = 0; k < schema_source->size(); ++k) {
                    auto col_name = schema_source->GetColumnName(k);
                    auto col_ref = node_manager->MakeColumnRefNode(
                        col_name, all_expr->GetRelationName());
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
    CHECK_TRUE(fn_info.IsValid(), kCodegenError);
    codegen::CodeGenContext codegen_ctx(module_, fn_info.schemas_ctx(),
                                        node_manager_);
    codegen::RowFnLetIRBuilder builder(&codegen_ctx);
    return builder.Build(fn_info.fn_name(), fn_info.fn_def(),
                         fn_info.GetPrimaryFrame(), fn_info.GetFrames(),
                         *fn_info.fn_schema());
}

bool BatchModeTransformer::AddDefaultPasses() {
    AddPass(kPassColumnProjectsOptimized);
    AddPass(kPassFilterOptimized);
    AddPass(kPassLeftJoinOptimized);
    AddPass(kPassGroupAndSortOptimized);
    AddPass(kPassLimitOptimized);
    AddPass(kPassClusterOptimized);
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
            auto group_op = dynamic_cast<PhysicalGroupNode*>(depend);
            CHECK_TRUE(group_op != nullptr, kPlanError,
                       "Can not create group agg after non group op");
            PhysicalGroupAggrerationNode* agg_op = nullptr;
            CHECK_STATUS(CreateOp<PhysicalGroupAggrerationNode>(
                &agg_op, depend, column_projects, group_op->group_.keys()));
            *output = agg_op;
            break;
        }
        case kWindowAggregation: {
            PhysicalWindowAggrerationNode* window_agg_op = nullptr;
            CHECK_STATUS(CreateOp<PhysicalWindowAggrerationNode>(
                &window_agg_op, depend, column_projects, project_list->w_ptr_,
                append_input));
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
            if (project_list->is_window_agg_) {
                CHECK_STATUS(
                    CheckWindow(project_list->w_ptr_, depend->schemas_ctx()));
                return CreatePhysicalProjectNode(kWindowAggregation, depend,
                                                 project_list, append_input,
                                                 output);
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
            case kPassColumnProjectsOptimized: {
                SimpleProjectOptimized pass(&plan_ctx_);
                transformed = pass.Apply(cur_op, &new_op);
                break;
            }
            case kPassFilterOptimized: {
                ConditionOptimized pass(&plan_ctx_);
                transformed = pass.Apply(cur_op, &new_op);
                break;
            }
            case kPassGroupAndSortOptimized: {
                GroupAndSortOptimized pass(&plan_ctx_);
                transformed = pass.Apply(cur_op, &new_op);
                break;
            }
            case kPassLeftJoinOptimized: {
                if (catalog_->IndexSupport()) {
                    LeftJoinOptimized pass(&plan_ctx_);
                    transformed = pass.Apply(cur_op, &new_op);
                }
                break;
            }
            case kPassClusterOptimized: {
                if (cluster_optimized_mode_) {
                    ClusterOptimized pass(&plan_ctx_);
                    transformed = pass.Apply(cur_op, &new_op);
                }
                break;
            }
            case kPassLimitOptimized: {
                LimitOptimized pass(&plan_ctx_);
                transformed = pass.Apply(cur_op, &new_op);
                break;
            }
            default: {
                LOG(WARNING) << "can't not handle pass: "
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
        LOG(WARNING) << "Final transformed result is null";
    }
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
                node::ExprListNullOrEmpty(join.right_sort_.orders()->order_by_),
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
                   node::ExprListNullOrEmpty(window.sort().orders()->order_by_),
               kPlanError, "Window node hasn't been optimzied")
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
    const ::fesql::node::PlanNodeList& trees,
    ::fesql::vm::PhysicalOpNode** output) {
    CHECK_TRUE(module_ != nullptr && !trees.empty(), kPlanError,
               "Module or logical trees is empty");

    auto it = trees.begin();
    for (; it != trees.end(); ++it) {
        const ::fesql::node::PlanNode* node = *it;
        switch (node->GetType()) {
            case ::fesql::node::kPlanTypeFuncDef: {
                const ::fesql::node::FuncDefPlanNode* func_def_plan =
                    dynamic_cast<const ::fesql::node::FuncDefPlanNode*>(node);
                CHECK_STATUS(GenFnDef(func_def_plan),
                             "Fail to compile user function def");
                *output = nullptr;
                break;
            }
            case ::fesql::node::kPlanTypeUnion:
            case ::fesql::node::kPlanTypeQuery: {
                PhysicalOpNode* physical_plan = nullptr;
                CHECK_STATUS(TransformQueryPlan(node, &physical_plan),
                             "Fail to transform query plan to physical plan");
                DLOG(INFO) << "Before optimization: \n"
                           << physical_plan->GetTreeString();

                PhysicalOpNode* optimized_physical_plan = nullptr;
                ApplyPasses(physical_plan, &optimized_physical_plan);

                DLOG(INFO) << "After optimization: \n"
                           << optimized_physical_plan->GetTreeString();
                if (performance_sensitive_mode_) {
                    CHECK_STATUS(
                        ValidateIndexOptimization(optimized_physical_plan),
                        "Fail to support physical plan in "
                        "performance sensitive mode: \n",
                        optimized_physical_plan->GetTreeString());
                }
                std::set<PhysicalOpNode*> node_visited_dict;
                CHECK_STATUS(
                    InitFnInfo(optimized_physical_plan, &node_visited_dict),
                    "Fail to generate functions for physical plan");
                *output = optimized_physical_plan;
                break;
            }
            case ::fesql::node::kPlanTypeCreateSp: {
                const ::fesql::node::CreateProcedurePlanNode* sp_plan =
                    dynamic_cast<const ::fesql::node::CreateProcedurePlanNode*>(
                        node);
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

    ::fesql::codegen::FnIRBuilder builder(module_);
    ::llvm::Function* fn = nullptr;
    Status status;
    bool ok = builder.Build(fn_plan->fn_def_, &fn, status);
    CHECK_TRUE(ok, kCodegenError, "Fail to codegen function: " + status.str());

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
        return plan_ctx_.InitFnDef(&expr_list, schemas_ctx, true, filter);
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

Status BatchModeTransformer::GenSort(Sort* sort,
                                     const SchemasContext* schemas_ctx) {
    if (nullptr != sort->orders_ &&
        !node::ExprListNullOrEmpty(sort->orders()->order_by())) {
        return plan_ctx_.InitFnDef(sort->orders()->order_by(), schemas_ctx,
                                   true, sort);
    }
    return Status::OK();
}

Status BatchModeTransformer::GenRange(Range* range,
                                      const SchemasContext* schemas_ctx) {
    if (nullptr != range->range_key_) {
        node::ExprListNode expr_list;
        expr_list.AddChild(const_cast<node::ExprNode*>(range->range_key_));
        return plan_ctx_.InitFnDef(&expr_list, schemas_ctx, true, range);
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

Status BatchModeTransformer::CheckTimeOrIntegerOrderColumn(
    const node::OrderByNode* orders, const vm::SchemasContext* schemas_ctx) {
    if (nullptr != orders && !node::ExprListNullOrEmpty(orders->order_by_)) {
        if (1u != orders->order_by_->children_.size()) {
            return base::Status(
                common::kPlanError,
                "Invalid Order Column: can't support multi order");
        }

        auto order = orders->order_by_->children_[0];
        if (node::kExprColumnRef != order->expr_type_) {
            return base::Status(
                common::kPlanError,
                "Invalid Order Column: can't support expression order");
        }
        size_t schema_idx;
        size_t col_idx;
        CHECK_STATUS(schemas_ctx->ResolveColumnRefIndex(
            dynamic_cast<node::ColumnRefNode*>(order), &schema_idx, &col_idx));

        auto col_type = schemas_ctx->GetSchema(schema_idx)->Get(col_idx).type();
        switch (col_type) {
            case type::kInt16:
            case type::kInt32:
            case type::kInt64:
            case type::kTimestamp: {
                return Status::OK();
            }
            default: {
                return Status(kPlanError, "Invalid Order column type : " +
                                              fesql::type::Type_Name(col_type));
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
                   !node::ExprListNullOrEmpty(w_ptr->GetOrders()->order_by_),
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

bool GroupAndSortOptimized::KeysFilterOptimized(
    const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* group,
    Key* hash, PhysicalOpNode** new_in) {
    if (nullptr == group || nullptr == hash || !group->ValidKey()) {
        LOG(INFO) << "Fail KeysFilterOptimized when window filter key or "
                     "index key is empty";
        return false;
    }
    if (kPhysicalOpDataProvider == in->GetOpType()) {
        auto scan_op = dynamic_cast<PhysicalDataProviderNode*>(in);
        if (kProviderTypeTable == scan_op->provider_type_) {
            std::string index_name;
            const std::string& table_name = scan_op->table_handler_->GetName();
            auto& index_hint = scan_op->table_handler_->GetIndex();

            size_t key_num = group->keys()->GetChildNum();
            std::vector<bool> bitmap(key_num, false);
            if (!TransformGroupExpr(root_schemas_ctx, group->keys(), table_name,
                                    index_hint, &index_name, &bitmap)) {
                return false;
            }

            PhysicalPartitionProviderNode* scan_index_op = nullptr;
            Status status = plan_ctx_->CreateOp<PhysicalPartitionProviderNode>(
                &scan_index_op, scan_op, index_name);
            if (!status.isOK()) {
                LOG(INFO) << "Fail to create scan index op: " << status;
                return false;
            }
            auto new_groups = node_manager_->MakeExprList();
            auto new_keys = node_manager_->MakeExprList();
            for (size_t i = 0; i < bitmap.size(); ++i) {
                if (bitmap[i]) {
                    new_keys->AddChild(group->keys()->GetChild(i));
                } else {
                    new_groups->AddChild(group->keys()->GetChild(i));
                }
            }
            group->set_keys(new_groups);
            hash->set_keys(new_keys);
            *new_in = scan_index_op;
            return true;
        }
    } else if (kPhysicalOpSimpleProject == in->GetOpType()) {
        auto simple_project = dynamic_cast<PhysicalSimpleProjectNode*>(in);
        PhysicalOpNode* new_depend;
        if (!KeysFilterOptimized(root_schemas_ctx,
                                 simple_project->producers()[0], group, hash,
                                 &new_depend)) {
            return false;
        }
        PhysicalSimpleProjectNode* simple_project_op = nullptr;
        Status status = plan_ctx_->CreateOp<PhysicalSimpleProjectNode>(
            &simple_project_op, new_depend, simple_project->project());
        if (!status.isOK()) {
            LOG(WARNING) << "Fail to create simple project op: " << status;
            return false;
        }
        *new_in = simple_project_op;
        return true;
    } else if (kPhysicalOpRename == in->GetOpType()) {
        PhysicalOpNode* new_depend;
        if (!KeysFilterOptimized(root_schemas_ctx, in->producers()[0], group,
                                 hash, &new_depend)) {
            return false;
        }
        PhysicalRenameNode* rename_op = nullptr;
        Status status = plan_ctx_->CreateOp<PhysicalRenameNode>(
            &rename_op, new_depend,
            dynamic_cast<PhysicalRenameNode*>(in)->name_);
        if (!status.isOK()) {
            LOG(WARNING) << "Fail to create rename op: " << status;
            return false;
        }
        *new_in = rename_op;
        return true;
    }
    return false;
}

bool GroupAndSortOptimized::JoinKeysOptimized(
    const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Join* join,
    PhysicalOpNode** new_in) {
    if (nullptr == join) {
        return false;
    }
    return FilterOptimized(root_schemas_ctx, in, join, new_in);
}

bool GroupAndSortOptimized::FilterOptimized(
    const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Filter* filter,
    PhysicalOpNode** new_in) {
    if (nullptr == filter ||
        node::ExprListNullOrEmpty(filter->right_key_.keys())) {
        return false;
    }

    if (kPhysicalOpDataProvider == in->GetOpType()) {
        auto scan_op = dynamic_cast<PhysicalDataProviderNode*>(in);
        if (kProviderTypeTable == scan_op->provider_type_) {
            std::string index_name;
            const std::string& table_name = scan_op->table_handler_->GetName();
            auto& index_hint = scan_op->table_handler_->GetIndex();
            const node::ExprListNode* right_partition =
                filter->right_key_.keys();

            size_t key_num = right_partition->GetChildNum();
            std::vector<bool> bitmap(key_num, false);
            if (!TransformGroupExpr(root_schemas_ctx, right_partition,
                                    table_name, index_hint, &index_name,
                                    &bitmap)) {
                return false;
            }

            PhysicalPartitionProviderNode* partition_op = nullptr;
            Status status = plan_ctx_->CreateOp<PhysicalPartitionProviderNode>(
                &partition_op, scan_op, index_name);
            if (!status.isOK()) {
                LOG(WARNING) << "Fail to create partition op: " << status;
                return false;
            }
            auto new_left_keys = node_manager_->MakeExprList();
            auto new_right_keys = node_manager_->MakeExprList();
            auto new_index_keys = node_manager_->MakeExprList();
            for (size_t i = 0; i < bitmap.size(); ++i) {
                auto left_key = filter->left_key_.keys()->GetChild(i);
                auto right_key = filter->right_key_.keys()->GetChild(i);
                if (bitmap[i]) {
                    new_index_keys->AddChild(left_key);
                } else {
                    new_left_keys->AddChild(left_key);
                    new_right_keys->AddChild(right_key);
                }
            }
            filter->right_key_.set_keys(new_right_keys);
            filter->index_key_.set_keys(new_index_keys);
            filter->left_key_.set_keys(new_left_keys);
            *new_in = partition_op;
            return true;
        }
    } else if (kPhysicalOpSimpleProject == in->GetOpType()) {
        auto simple_project = dynamic_cast<PhysicalSimpleProjectNode*>(in);
        PhysicalOpNode* new_depend;
        if (!FilterOptimized(root_schemas_ctx, simple_project->producers()[0],
                             filter, &new_depend)) {
            return false;
        }
        PhysicalSimpleProjectNode* new_simple_op = nullptr;
        Status status = plan_ctx_->CreateOp<PhysicalSimpleProjectNode>(
            &new_simple_op, new_depend, simple_project->project());
        if (!status.isOK()) {
            LOG(WARNING) << "Fail to create simple project op: " << status;
            return false;
        }
        *new_in = new_simple_op;
        return true;
    } else if (kPhysicalOpRename == in->GetOpType()) {
        PhysicalOpNode* new_depend;
        if (!FilterOptimized(root_schemas_ctx, in->producers()[0], filter,
                             &new_depend)) {
            return false;
        }
        PhysicalRenameNode* new_op = nullptr;
        Status status = plan_ctx_->CreateOp<PhysicalRenameNode>(
            &new_op, new_depend, dynamic_cast<PhysicalRenameNode*>(in)->name_);
        if (!status.isOK()) {
            LOG(WARNING) << "Fail to create rename op: " << status;
            return false;
        }
        *new_in = new_op;
        return true;
    }
    return false;
}

bool GroupAndSortOptimized::GroupOptimized(
    const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* group,
    PhysicalOpNode** new_in) {
    if (nullptr == group) {
        return false;
    }

    if (kPhysicalOpDataProvider == in->GetOpType()) {
        auto scan_op = dynamic_cast<PhysicalDataProviderNode*>(in);
        if (kProviderTypeTable == scan_op->provider_type_) {
            std::string index_name;
            auto& index_hint = scan_op->table_handler_->GetIndex();
            const std::string& table_name = scan_op->table_handler_->GetName();

            size_t key_num = group->keys()->GetChildNum();
            std::vector<bool> bitmap(key_num, false);
            if (!TransformGroupExpr(root_schemas_ctx, group->keys(), table_name,
                                    index_hint, &index_name, &bitmap)) {
                return false;
            }
            auto new_groups = node_manager_->MakeExprList();
            for (size_t i = 0; i < bitmap.size(); ++i) {
                if (!bitmap[i]) {
                    new_groups->AddChild(group->keys()->GetChild(i));
                }
            }
            group->set_keys(new_groups);

            PhysicalPartitionProviderNode* partition_op = nullptr;
            Status status = plan_ctx_->CreateOp<PhysicalPartitionProviderNode>(
                &partition_op, scan_op, index_name);
            if (!status.isOK()) {
                LOG(WARNING) << "Fail to create partition op: " << status;
                return false;
            }
            *new_in = partition_op;
            return true;
        }
    } else if (kPhysicalOpSimpleProject == in->GetOpType()) {
        auto simple_project = dynamic_cast<PhysicalSimpleProjectNode*>(in);
        PhysicalOpNode* new_depend;
        if (!GroupOptimized(root_schemas_ctx, simple_project->producers()[0],
                            group, &new_depend)) {
            return false;
        }
        PhysicalSimpleProjectNode* new_simple_op = nullptr;
        Status status = plan_ctx_->CreateOp<PhysicalSimpleProjectNode>(
            &new_simple_op, new_depend, simple_project->project());
        if (!status.isOK()) {
            LOG(WARNING) << "Fail to create simple project op: " << status;
            return false;
        }
        *new_in = new_simple_op;
        return true;
    } else if (kPhysicalOpRename == in->GetOpType()) {
        PhysicalOpNode* new_depend;
        if (!GroupOptimized(root_schemas_ctx, in->producers()[0], group,
                            &new_depend)) {
            return false;
        }
        PhysicalRenameNode* new_op = nullptr;
        Status status = plan_ctx_->CreateOp<PhysicalRenameNode>(
            &new_op, new_depend, dynamic_cast<PhysicalRenameNode*>(in)->name_);
        if (!status.isOK()) {
            LOG(WARNING) << "Fail to create rename op: " << status;
            return false;
        }
        *new_in = new_op;
        return true;
    }
    return false;
}

bool GroupAndSortOptimized::SortOptimized(
    const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Sort* sort) {
    if (nullptr == sort) {
        return false;
    }
    if (kPhysicalOpDataProvider == in->GetOpType()) {
        auto scan_op = dynamic_cast<PhysicalDataProviderNode*>(in);
        if (kProviderTypePartition != scan_op->provider_type_) {
            return false;
        }
        auto partition_provider =
            dynamic_cast<PhysicalPartitionProviderNode*>(scan_op);
        const node::OrderByNode* new_orders = nullptr;

        auto& index_hint = partition_provider->table_handler_->GetIndex();
        std::string index_name = partition_provider->index_name_;
        auto index_st = index_hint.at(index_name);
        TransformOrderExpr(root_schemas_ctx, sort->orders(),
                           *(scan_op->table_handler_->GetSchema()), index_st,
                           &new_orders);
        sort->set_orders(new_orders);
        return true;
    } else if (kPhysicalOpSimpleProject == in->GetOpType()) {
        auto simple_project = dynamic_cast<PhysicalSimpleProjectNode*>(in);
        return SortOptimized(root_schemas_ctx, simple_project->producers()[0],
                             sort);
    } else if (kPhysicalOpRename == in->GetOpType()) {
        return SortOptimized(root_schemas_ctx, in->producers()[0], sort);
    }
    return false;
}

bool GroupAndSortOptimized::Transform(PhysicalOpNode* in,
                                      PhysicalOpNode** output) {
    *output = in;
    switch (in->GetOpType()) {
        case kPhysicalOpGroupBy: {
            PhysicalGroupNode* group_op = dynamic_cast<PhysicalGroupNode*>(in);
            PhysicalOpNode* new_producer;
            if (!GroupOptimized(group_op->schemas_ctx(),
                                group_op->GetProducer(0), &group_op->group_,
                                &new_producer)) {
                return false;
            }
            if (!ResetProducer(plan_ctx_, group_op, 0, new_producer)) {
                return false;
            }
            if (!group_op->Valid()) {
                *output = group_op->producers()[0];
            }
            return true;
        }
        case kPhysicalOpProject: {
            auto project_op = dynamic_cast<PhysicalProjectNode*>(in);
            if (kWindowAggregation == project_op->project_type_) {
                auto window_agg_op =
                    dynamic_cast<PhysicalWindowAggrerationNode*>(project_op);
                PhysicalOpNode* input = window_agg_op->GetProducer(0);

                PhysicalOpNode* new_producer;
                if (!window_agg_op->instance_not_in_window()) {
                    if (GroupOptimized(input->schemas_ctx(), input,
                                       &window_agg_op->window_.partition_,
                                       &new_producer)) {
                        input = new_producer;
                        if (!ResetProducer(plan_ctx_, window_agg_op, 0,
                                           input)) {
                            return false;
                        }
                    }
                }

                // must prepare for window join column infer
                auto& window_joins = window_agg_op->window_joins();
                auto& window_unions = window_agg_op->window_unions();
                window_agg_op->InitJoinList(plan_ctx_);
                auto& joined_op_list_ = window_agg_op->joined_op_list_;
                PhysicalOpNode* final_joined =
                    joined_op_list_.empty() ? input : joined_op_list_.back();

                if (!window_agg_op->instance_not_in_window()) {
                    SortOptimized(final_joined->schemas_ctx(), input,
                                  &window_agg_op->window_.sort_);
                }

                if (!window_joins.Empty()) {
                    size_t join_idx = 0;
                    for (auto& window_join : window_joins.window_joins()) {
                        PhysicalOpNode* cur_joined = joined_op_list_[join_idx];

                        PhysicalOpNode* new_join_right;
                        if (JoinKeysOptimized(
                                cur_joined->schemas_ctx(), window_join.first,
                                &window_join.second, &new_join_right)) {
                            window_join.first = new_join_right;
                        }
                        SortOptimized(cur_joined->schemas_ctx(),
                                      window_join.first,
                                      &window_join.second.right_sort_);
                        join_idx += 1;
                    }
                }
                if (!window_unions.Empty()) {
                    for (auto& window_union : window_unions.window_unions_) {
                        PhysicalOpNode* new_producer;

                        if (GroupOptimized(window_union.first->schemas_ctx(),
                                           window_union.first,
                                           &window_union.second.partition_,
                                           &new_producer)) {
                            window_union.first = new_producer;
                        }
                        SortOptimized(window_union.first->schemas_ctx(),
                                      window_union.first,
                                      &window_union.second.sort_);
                    }
                }
                return true;
            }
            break;
        }
        case kPhysicalOpRequestUnion: {
            PhysicalRequestUnionNode* union_op =
                dynamic_cast<PhysicalRequestUnionNode*>(in);
            PhysicalOpNode* new_producer;

            if (!union_op->instance_not_in_window()) {
                if (KeysFilterOptimized(
                        union_op->schemas_ctx(), union_op->GetProducer(1),
                        &union_op->window_.partition_,
                        &union_op->window_.index_key_, &new_producer)) {
                    if (!ResetProducer(plan_ctx_, union_op, 1, new_producer)) {
                        return false;
                    }
                }
                SortOptimized(union_op->schemas_ctx(), union_op->GetProducer(1),
                              &union_op->window_.sort_);
            }

            if (!union_op->window_unions().Empty()) {
                for (auto& window_union :
                     union_op->window_unions_.window_unions_) {
                    PhysicalOpNode* new_producer;
                    auto& window = window_union.second;
                    if (KeysFilterOptimized(
                            window_union.first->schemas_ctx(),
                            window_union.first, &window.partition_,
                            &window.index_key_, &new_producer)) {
                        window_union.first = new_producer;
                    }
                    SortOptimized(window_union.first->schemas_ctx(),
                                  window_union.first, &window.sort_);
                }
            }
            return true;
        }
        case kPhysicalOpRequestJoin: {
            PhysicalRequestJoinNode* join_op =
                dynamic_cast<PhysicalRequestJoinNode*>(in);
            PhysicalOpNode* new_producer;
            // Optimized Right Table Partition
            if (!JoinKeysOptimized(join_op->schemas_ctx(),
                                   join_op->GetProducer(1), &join_op->join_,
                                   &new_producer)) {
                return false;
            }
            if (!ResetProducer(plan_ctx_, join_op, 1, new_producer)) {
                return false;
            }
            SortOptimized(join_op->schemas_ctx(), join_op->GetProducer(1),
                          &join_op->join_.right_sort_);
            return true;
        }
        case kPhysicalOpJoin: {
            PhysicalJoinNode* join_op = dynamic_cast<PhysicalJoinNode*>(in);
            PhysicalOpNode* new_producer;
            // Optimized Right Table Partition
            if (!JoinKeysOptimized(join_op->schemas_ctx(),
                                   join_op->GetProducer(1), &join_op->join_,
                                   &new_producer)) {
                return false;
            }
            if (!ResetProducer(plan_ctx_, join_op, 1, new_producer)) {
                return false;
            }
            SortOptimized(join_op->schemas_ctx(), join_op->GetProducer(1),
                          &join_op->join_.right_sort_);
            return true;
        }
        case kPhysicalOpFilter: {
            PhysicalFilterNode* filter_op =
                dynamic_cast<PhysicalFilterNode*>(in);
            PhysicalOpNode* new_producer;
            if (FilterOptimized(filter_op->schemas_ctx(),
                                filter_op->GetProducer(0), &filter_op->filter_,
                                &new_producer)) {
                if (!ResetProducer(plan_ctx_, filter_op, 0, new_producer)) {
                    return false;
                }
            }
        }
        default: {
            return false;
        }
    }
    return false;
}

/**
 * Resolve column reference to possible source table's column name
 */
static bool ResolveColumnToSourceColumnName(const node::ColumnRefNode* col,
                                            const SchemasContext* schemas_ctx,
                                            std::string* source_name) {
    // use detailed column resolve utility
    size_t column_id;
    int path_idx;
    size_t child_column_id;
    size_t source_column_id;
    const PhysicalOpNode* source;
    Status status = schemas_ctx->ResolveColumnID(
        col->GetRelationName(), col->GetColumnName(), &column_id, &path_idx,
        &child_column_id, &source_column_id, &source);

    // try loose the relation
    if (!status.isOK() && !col->GetRelationName().empty()) {
        status = schemas_ctx->ResolveColumnID(
            "", col->GetColumnName(), &column_id, &path_idx, &child_column_id,
            &source_column_id, &source);
    }

    if (!status.isOK()) {
        LOG(WARNING) << "Illegal index column: " << col->GetExprString();
        return false;
    }
    if (source == nullptr || source->GetOpType() != kPhysicalOpDataProvider) {
        LOG(WARNING) << "Index column is not from any source table: "
                     << col->GetExprString();
        return false;
    }
    status = source->schemas_ctx()->ResolveColumnNameByID(source_column_id,
                                                          source_name);
    if (!status.isOK()) {
        LOG(WARNING) << "Illegal source column id #" << source_column_id
                     << " for index column " << col->GetExprString();
        return false;
    }
    return true;
}

bool GroupAndSortOptimized::TransformGroupExpr(
    const SchemasContext* root_schemas_ctx, const node::ExprListNode* groups,
    const std::string& table_name, const IndexHint& index_hint,
    std::string* index_name, std::vector<bool>* output_bitmap) {
    if (nullptr == groups || nullptr == output_bitmap ||
        nullptr == index_name) {
        LOG(WARNING) << "fail to transform group expr : group exor or output "
                        "or index_name ptr is null";
        return false;
    }

    DLOG(INFO) << "keys optimized: " << node::ExprString(groups);
    std::vector<std::string> columns;
    std::map<size_t, size_t> result_bitmap_mapping;

    for (size_t i = 0; i < groups->children_.size(); ++i) {
        auto group = groups->children_[i];
        switch (group->expr_type_) {
            case node::kExprColumnRef: {
                auto column = dynamic_cast<node::ColumnRefNode*>(group);
                std::string source_column_name;
                if (!ResolveColumnToSourceColumnName(column, root_schemas_ctx,
                                                     &source_column_name)) {
                    return false;
                }
                result_bitmap_mapping[columns.size()] = i;
                columns.push_back(source_column_name);
                break;
            }
            default: {
                break;
            }
        }
    }

    if (columns.empty()) {
        return false;
    }

    std::vector<bool> match_bitmap;
    std::vector<bool> state_bitmap(columns.size(), true);
    if (!MatchBestIndex(columns, table_name, index_hint, &state_bitmap,
                        index_name, &match_bitmap)) {
        return false;
    }
    if (match_bitmap.size() != columns.size()) {
        return false;
    }
    for (size_t i = 0; i < columns.size(); ++i) {
        if (match_bitmap[i]) {
            size_t origin_idx = result_bitmap_mapping[i];
            (*output_bitmap)[origin_idx] = true;
        }
    }
    return true;
}

bool GroupAndSortOptimized::MatchBestIndex(
    const std::vector<std::string>& columns, const std::string& table_name,
    const IndexHint& index_hint, std::vector<bool>* bitmap_ptr,
    std::string* index_name, std::vector<bool>* index_bitmap) {
    if (nullptr == bitmap_ptr || nullptr == index_name) {
        LOG(WARNING)
            << "fail to match best index: bitmap or index_name ptr is null";
        return false;
    }
    std::set<std::string> column_set;
    auto& bitmap = *bitmap_ptr;
    for (size_t i = 0; i < columns.size(); ++i) {
        if (bitmap[i]) {
            column_set.insert(columns[i]);
        }
    }

    for (auto iter = index_hint.cbegin(); iter != index_hint.cend(); iter++) {
        IndexSt index = iter->second;
        std::set<std::string> keys;
        for (auto key_iter = index.keys.cbegin(); key_iter != index.keys.cend();
             key_iter++) {
            keys.insert(key_iter->name);
        }

        if (column_set == keys) {
            *index_name = index.name;
            *index_bitmap = bitmap;
            return true;
        }
    }

    std::string best_index_name;
    std::vector<bool> best_index_bitmap;

    bool succ = false;
    for (size_t i = 0; i < bitmap.size(); ++i) {
        if (bitmap[i]) {
            bitmap[i] = false;
            std::string name;
            std::vector<bool> sub_best_bitmap;
            if (MatchBestIndex(columns, table_name, index_hint, bitmap_ptr,
                               &name, &sub_best_bitmap)) {
                succ = true;
                if (best_index_name.empty()) {
                    best_index_name = name;
                    best_index_bitmap = sub_best_bitmap;
                } else {
                    auto org_index = index_hint.at(best_index_name);
                    auto new_index = index_hint.at(name);
                    if (org_index.keys.size() < new_index.keys.size()) {
                        best_index_name = name;
                        best_index_bitmap = sub_best_bitmap;
                    }
                }
            }
            bitmap[i] = true;
        }
    }
    *index_name = best_index_name;
    *index_bitmap = best_index_bitmap;
    return succ;
}

bool ConditionOptimized::JoinConditionOptimized(PhysicalBinaryNode* in,
                                                Join* join) {
    if (2 != in->producers().size()) {
        LOG(WARNING)
            << "Fail to Join Condition Optimized: input produces size isn't 2";
        return false;
    }
    node::ExprListNode and_conditions;
    if (!TransfromAndConditionList(join->condition_.condition_,
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
    if (!TransfromAndConditionList(filter->condition_.condition_,
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
        case kPhysicalOpJoin: {
            PhysicalJoinNode* join_op = dynamic_cast<PhysicalJoinNode*>(in);
            return JoinConditionOptimized(join_op, &join_op->join_);
        }
        case kPhysicalOpRequestJoin: {
            PhysicalRequestJoinNode* join_op =
                dynamic_cast<PhysicalRequestJoinNode*>(in);
            return JoinConditionOptimized(join_op, &join_op->join_);
        }
        case kPhysicalOpFilter: {
            PhysicalFilterNode* filter_op =
                dynamic_cast<PhysicalFilterNode*>(in);
            return FilterConditionOptimized(filter_op, &filter_op->filter_);
        }
        default: {
            return false;
        }
    }
}

// Transform condition expression some sub conditions
// e.g.
// condition : sub_expr1 and sub_expr2 and sub expr3
// and_condition_list [sub_expr1, sub_expr2, sub_exor3]
bool ConditionOptimized::TransfromAndConditionList(
    const node::ExprNode* condition, node::ExprListNode* and_condition_list) {
    if (nullptr == condition) {
        LOG(WARNING) << "Skip ConditionOptimized: conditions: null condition";
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
// Left Expr should belongs to first schema
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

bool LimitOptimized::Transform(PhysicalOpNode* in, PhysicalOpNode** output) {
    *output = in;
    if (kPhysicalOpLimit != in->GetOpType()) {
        return false;
    }

    auto limit_op = dynamic_cast<PhysicalLimitNode*>(in);

    if (ApplyLimitCnt(in->producers()[0], limit_op->GetLimitCnt())) {
        limit_op->SetLimitOptimized(true);
        return true;
    } else {
        return false;
    }
}

bool LimitOptimized::ApplyLimitCnt(PhysicalOpNode* node, int32_t limit_cnt) {
    if (kPhysicalOpLimit == node->GetOpType()) {
        auto limit_op = dynamic_cast<PhysicalLimitNode*>(node);
        if (0 == node->GetLimitCnt() || limit_op->GetLimitCnt() > limit_cnt) {
            if (limit_op->GetLimitOptimized()) {
                return ApplyLimitCnt(node->producers()[0], limit_cnt);
            } else {
                limit_op->SetLimitCnt(limit_cnt);
            }
        }
        return true;
    }
    if (node->producers().empty()) {
        return false;
    }
    if (node->GetOpType() == kPhysicalOpSimpleProject ||
        node->GetOpType() == kPhysicalOpRename) {
        return false;
    }
    if (node->is_block()) {
        if (0 == node->GetLimitCnt() || node->GetLimitCnt() > limit_cnt) {
            node->SetLimitCnt(limit_cnt);
        }
        return true;
    } else {
        if (false == ApplyLimitCnt(node->producers()[0], limit_cnt)) {
            if (0 == node->GetLimitCnt() || node->GetLimitCnt() > limit_cnt) {
                node->SetLimitCnt(limit_cnt);
                return true;
            }
        } else {
            return true;
        }
    }
    return false;
}

bool TransformUpPysicalPass::Apply(PhysicalOpNode* in, PhysicalOpNode** out) {
    if (nullptr == in || nullptr == out) {
        LOG(WARNING) << "fail to apply pass: input or output is null";
        return false;
    }
    auto producer = in->producers();
    for (size_t j = 0; j < producer.size(); ++j) {
        PhysicalOpNode* output = nullptr;
        if (Apply(producer[j], &output)) {
            if (!ResetProducer(plan_ctx_, in, j, output)) {
                return false;
            }
        }
    }
    in->ClearSchema();
    Status status = in->InitSchema(plan_ctx_);
    if (!status.isOK()) {
        LOG(WARNING) << "Reset schema failed: " << status;
        return false;
    }
    in->FinishSchema();
    return Transform(in, out);
}

bool GroupAndSortOptimized::TransformOrderExpr(
    const SchemasContext* schemas_ctx, const node::OrderByNode* order,
    const Schema& schema, const IndexSt& index_st,
    const node::OrderByNode** output) {
    *output = order;
    if (nullptr == order || nullptr == output) {
        LOG(WARNING)
            << "fail to optimize order expr : order expr or output is null";
        return false;
    }
    if (index_st.ts_pos == INVALID_POS) {
        LOG(WARNING) << "not set ts col";
        return false;
    }
    auto& ts_column = schema.Get(index_st.ts_pos);
    *output = order;
    int succ_match = -1;
    for (size_t i = 0; i < order->order_by()->GetChildNum(); ++i) {
        auto expr = order->order_by()->GetChild(i);
        if (expr->GetExprType() == node::kExprColumnRef) {
            auto column = dynamic_cast<node::ColumnRefNode*>(expr);
            std::string source_column_name;
            if (ResolveColumnToSourceColumnName(column, schemas_ctx,
                                                &source_column_name)) {
                if (ts_column.name() == source_column_name) {
                    succ_match = i;
                    break;
                }
            }
        }
    }
    if (succ_match >= 0) {
        node::ExprListNode* expr_list = node_manager_->MakeExprList();
        for (size_t i = 0; i < order->order_by()->GetChildNum(); ++i) {
            if (static_cast<size_t>(succ_match) != i) {
                expr_list->AddChild(order->order_by()->GetChild(i));
            }
        }
        *output = dynamic_cast<node::OrderByNode*>(
            node_manager_->MakeOrderByNode(expr_list, order->is_asc_));
        return true;
    } else {
        return false;
    }
}

bool LeftJoinOptimized::Transform(PhysicalOpNode* in, PhysicalOpNode** output) {
    if (nullptr == in) {
        LOG(WARNING) << "LeftJoin optimized skip: node is null";
        return false;
    }
    if (in->producers().empty() || nullptr == in->producers()[0] ||
        kPhysicalOpJoin != in->producers()[0]->GetOpType()) {
        return false;
    }
    PhysicalJoinNode* join_op =
        dynamic_cast<PhysicalJoinNode*>(in->producers()[0]);

    auto join_type = join_op->join().join_type();
    if (node::kJoinTypeLeft != join_type && node::kJoinTypeLast != join_type) {
        // skip optimized for other join type
        return false;
    }
    switch (in->GetOpType()) {
        case kPhysicalOpGroupBy: {
            auto group_op = dynamic_cast<PhysicalGroupNode*>(in);
            if (node::ExprListNullOrEmpty(group_op->group_.keys_)) {
                LOG(WARNING) << "Join optimized skip: groups is null or empty";
            }

            if (!CheckExprListFromSchema(
                    group_op->group_.keys_,
                    join_op->GetProducers()[0]->GetOutputSchema())) {
                return false;
            }
            auto group_expr = group_op->group_.keys_;
            // 符合优化条件
            PhysicalGroupNode* new_group_op = nullptr;
            Status status = plan_ctx_->CreateOp<PhysicalGroupNode>(
                &new_group_op, join_op->producers()[0], group_expr);
            if (!status.isOK()) {
                return false;
            }
            PhysicalJoinNode* new_join_op = nullptr;
            status = plan_ctx_->CreateOp<PhysicalJoinNode>(
                &new_join_op, new_group_op, join_op->GetProducers()[1],
                join_op->join_);
            if (!status.isOK()) {
                return false;
            }
            *output = new_join_op;
            return true;
        }
        case kPhysicalOpSortBy: {
            auto sort_op = dynamic_cast<PhysicalSortNode*>(in);
            if (nullptr == sort_op->sort_.orders_ ||
                node::ExprListNullOrEmpty(sort_op->sort_.orders_->order_by_)) {
                LOG(WARNING) << "Join optimized skip: order is null or empty";
            }
            if (!CheckExprListFromSchema(
                    sort_op->sort_.orders_->order_by_,
                    join_op->GetProducers()[0]->GetOutputSchema())) {
                return false;
            }
            // 符合优化条件
            PhysicalSortNode* new_order_op = nullptr;
            Status status = plan_ctx_->CreateOp<PhysicalSortNode>(
                &new_order_op, join_op->producers()[0], sort_op->sort_);
            if (!status.isOK()) {
                return false;
            }
            PhysicalJoinNode* new_join_op = nullptr;
            status = plan_ctx_->CreateOp<PhysicalJoinNode>(
                &new_join_op, new_order_op, join_op->GetProducers()[1],
                join_op->join_);
            if (!status.isOK()) {
                return false;
            }
            *output = new_join_op;
            return true;
        }
        case kPhysicalOpProject: {
            auto project_op = dynamic_cast<PhysicalProjectNode*>(in);
            if (kWindowAggregation != project_op->project_type_) {
                return false;
            }

            if (node::kJoinTypeLast != join_type) {
                LOG(WARNING) << "Window Join optimized skip: join type should "
                                "be LAST JOIN, but "
                             << node::JoinTypeName(join_type);
                return false;
            }
            auto window_agg_op =
                dynamic_cast<PhysicalWindowAggrerationNode*>(in);
            if (node::ExprListNullOrEmpty(
                    window_agg_op->window_.partition_.keys_) &&
                (nullptr == window_agg_op->window_.sort_.orders_ ||
                 node::ExprListNullOrEmpty(
                     window_agg_op->window_.sort_.orders_->order_by_))) {
                LOG(WARNING) << "Window Join optimized skip: both partition and"
                                "order are empty ";
                return false;
            }
            auto left_schemas_ctx = join_op->GetProducer(0)->schemas_ctx();
            if (!CheckExprDependOnChildOnly(
                     window_agg_op->window_.partition_.keys_, left_schemas_ctx)
                     .isOK()) {
                LOG(WARNING) << "Window Join optimized skip: partition keys "
                                "are resolved from secondary table";
                return false;
            }
            if (!CheckExprDependOnChildOnly(
                     window_agg_op->window_.sort_.orders_->order_by_,
                     left_schemas_ctx)
                     .isOK()) {
                LOG(WARNING) << "Window Join optimized skip: order keys are "
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

bool ClusterOptimized::SimplifyJoinLeftInput(
    PhysicalOpNode* join_op, const Join& join,
    const SchemasContext* joined_schema_ctx, PhysicalOpNode** output) {
    auto left = join_op->GetProducer(0);
    std::vector<const fesql::node::ExprNode*> columns;
    std::vector<std::string> column_names;
    join.ResolvedRelatedColumns(&columns);

    std::ostringstream oss;
    for (auto column : columns) {
        oss << node::ExprString(column) << ",";
    }
    LOG(INFO) << "join resolved related columns: \n" << oss.str();

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
            Status status = joined_schema_ctx->ResolveColumnID(
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
        LOG(WARNING) << "Simplify columns size == left output columns";
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
    DLOG(INFO) << "apply root node simplify!";
    *output = root_simplify_project_op;
    return true;
}

bool ClusterOptimized::Transform(PhysicalOpNode* in, PhysicalOpNode** output) {
    if (nullptr == in) {
        LOG(WARNING) << "cluster optimized skip: node is null";
        return false;
    }
    switch (in->GetOpType()) {
        case kPhysicalOpRequestJoin: {
            auto join_op = dynamic_cast<PhysicalRequestJoinNode*>(in);
            switch (join_op->join().join_type()) {
                case node::kJoinTypeLeft:
                case node::kJoinTypeLast: {
                    auto left = join_op->producers()[0];
                    auto right = join_op->producers()[1];
                    if (kSchemaTypeRow == right->GetOutputType()) {
                        DLOG(INFO)
                            << "request join optimized skip: row and row join";
                        return false;
                    }
                    auto simplify_left = left;
                    if (!SimplifyJoinLeftInput(join_op, join_op->join(),
                                               join_op->joined_schemas_ctx(),
                                               &simplify_left)) {
                        LOG(WARNING) << "Simplify join left input failed";
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
                        fesql::node::kJoinTypeConcat);
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
        case kPhysicalOpJoin: {
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
                        LOG(WARNING) << "Simplify join left input failed";
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

                    PhysicalJoinNode* concat_op = nullptr;
                    status = plan_ctx_->CreateOp<PhysicalJoinNode>(
                        &concat_op, left, join_right_only,
                        fesql::node::kJoinTypeConcat);
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

RequestModeTransformer::RequestModeTransformer(
    node::NodeManager* node_manager, const std::string& db,
    const std::shared_ptr<Catalog>& catalog, ::llvm::Module* module,
    udf::UDFLibrary* library, const std::set<size_t>& common_column_indices,
    const bool performance_sensitive, const bool cluster_optimized,
    const bool enable_batch_request_opt, bool enable_expr_opt)
    : BatchModeTransformer(node_manager, db, catalog, module, library,
                           performance_sensitive, cluster_optimized,
                           enable_expr_opt),
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
        fesql::node::ProjectListNode* project_list =
            dynamic_cast<fesql::node::ProjectListNode*>(*iter);

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
        &join, ops[0], ops[1], ::fesql::node::kJoinTypeConcat));

    for (size_t i = 2; i < ops.size(); ++i) {
        PhysicalRequestJoinNode* new_join = nullptr;
        CHECK_STATUS(CreateOp<PhysicalRequestJoinNode>(
            &new_join, join, ops[i], ::fesql::node::kJoinTypeConcat));
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
        CHECK_TRUE(table != nullptr, kPlanError,
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
            return CreatePhysicalProjectNode(
                kRowProject, new_depend, project_list, append_input, output);
        case kSchemaTypeGroup:
            return CreatePhysicalProjectNode(kGroupAggregation, new_depend,
                                             project_list, append_input,
                                             output);
        case kSchemaTypeTable:
            if (project_list->is_window_agg_) {
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
        LOG(WARNING) << "Final optimized result is null";
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
        LOG(WARNING) << "Fail to perform batch request optimization: "
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

static Status BuildColumnMapping(
    const node::ExprNode* outer_expr,
    const std::vector<node::ExprNode*>& inner_projects,
    const SchemasContext* schemas_ctx, passes::ExprReplacer* replacer) {
    for (size_t i = 0; i < outer_expr->GetChildNum(); ++i) {
        CHECK_STATUS(BuildColumnMapping(outer_expr->GetChild(i), inner_projects,
                                        schemas_ctx, replacer));
    }
    switch (outer_expr->GetExprType()) {
        case node::kExprColumnRef: {
            auto col_ref = dynamic_cast<const node::ColumnRefNode*>(outer_expr);
            size_t schema_idx;
            size_t col_idx;
            schemas_ctx->ResolveColumnRefIndex(col_ref, &schema_idx, &col_idx);
            CHECK_TRUE(schema_idx == 0, kPlanError,
                       "Simple project should output single schema");
            CHECK_TRUE(col_idx < inner_projects.size(), kPlanError,
                       "Column index out of bound");

            auto repl = inner_projects[col_idx];
            replacer->AddReplacement(col_ref, repl);
            break;
        }
        default:
            break;
    }
    return Status::OK();
}

bool SimpleProjectOptimized::Transform(PhysicalOpNode* in,
                                       PhysicalOpNode** output) {
    *output = in;
    switch (in->GetOpType()) {
        case kPhysicalOpSimpleProject: {
            if (nullptr != in->producers()[0] &&
                kPhysicalOpSimpleProject == in->producers()[0]->GetOpType()) {
                DLOG(INFO) << "Find consecutive simple projects";
                auto nm = plan_ctx_->node_manager();
                auto outer_project_op =
                    dynamic_cast<PhysicalSimpleProjectNode*>(in);
                auto inner_project_op =
                    dynamic_cast<PhysicalSimpleProjectNode*>(
                        in->GetProducer(0));

                auto outer_projects = outer_project_op->project();
                std::vector<node::ExprNode*> outer_exprs;
                for (size_t i = 0; i < outer_projects.size(); ++i) {
                    outer_exprs.push_back(
                        outer_projects.GetExpr(i)->DeepCopy(nm));
                }

                auto inner_projects = inner_project_op->project();
                std::vector<node::ExprNode*> inner_exprs;
                for (size_t i = 0; i < inner_projects.size(); ++i) {
                    inner_exprs.push_back(
                        inner_projects.GetExpr(i)->DeepCopy(nm));
                }

                Status status;
                passes::ExprReplacer replacer;
                for (size_t i = 0; i < outer_projects.size(); ++i) {
                    status = BuildColumnMapping(outer_exprs[i], inner_exprs,
                                                inner_project_op->schemas_ctx(),
                                                &replacer);
                    if (!status.isOK()) {
                        LOG(WARNING)
                            << "Fail to merge simple projects: " << status;
                        return false;
                    }
                }

                ColumnProjects new_projects;
                for (size_t i = 0; i < outer_projects.size(); ++i) {
                    node::ExprNode* new_expr;
                    status = replacer.Replace(outer_exprs[i], &new_expr);
                    if (!status.isOK()) {
                        LOG(WARNING)
                            << "Fail to merge simple projects: " << status;
                        return false;
                    }
                    new_projects.Add(outer_projects.GetName(i), new_expr,
                                     outer_projects.GetFrame(i));
                }
                PhysicalSimpleProjectNode* new_project_op = nullptr;
                status = plan_ctx_->CreateOp<PhysicalSimpleProjectNode>(
                    &new_project_op, inner_project_op->GetProducer(0),
                    new_projects);
                if (!status.isOK()) {
                    LOG(WARNING) << "Fail to merge simple projects: " << status;
                    return false;
                }
                *output = new_project_op;
                return true;
            }
        }
        default: {
            return false;
        }
    }
}

}  // namespace vm
}  // namespace fesql
