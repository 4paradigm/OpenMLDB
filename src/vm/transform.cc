/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * transform.cc
 *
 * Author: chenjing
 * Date: 2020/3/13
 *--------------------------------------------------------------------------
 **/
#include "vm/transform.h"
#include <set>
#include <stack>
#include <unordered_map>
#include "codegen/context.h"
#include "codegen/fn_ir_builder.h"
#include "codegen/fn_let_ir_builder.h"
#include "vm/physical_op.h"
#include "vm/schemas_context.h"

#include "passes/lambdafy_projects.h"
#include "passes/resolve_fn_and_attrs.h"

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
    udf::UDFLibrary* library)
    : node_manager_(node_manager),
      db_(db),
      catalog_(catalog),
      module_(module),
      id_(0),
      performance_sensitive_mode_(false),
      library_(library) {}

BatchModeTransformer::BatchModeTransformer(
    node::NodeManager* node_manager, const std::string& db,
    const std::shared_ptr<Catalog>& catalog, ::llvm::Module* module,
    udf::UDFLibrary* library, bool performance_sensitive)
    : node_manager_(node_manager),
      db_(db),
      catalog_(catalog),
      module_(module),
      id_(0),
      performance_sensitive_mode_(performance_sensitive),
      library_(library) {}
BatchModeTransformer::~BatchModeTransformer() {}

bool BatchModeTransformer::TransformPlanOp(const ::fesql::node::PlanNode* node,
                                           ::fesql::vm::PhysicalOpNode** ouput,
                                           ::fesql::base::Status& status) {
    if (nullptr == node || nullptr == ouput) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }
    LogicalOp logical_op = LogicalOp(node);
    auto map_iter = op_map_.find(logical_op);
    // logical plan node already exist
    if (map_iter != op_map_.cend()) {
        *ouput = map_iter->second;
        return true;
    }

    ::fesql::vm::PhysicalOpNode* op = nullptr;
    bool ok = true;
    switch (node->type_) {
        case node::kPlanTypeLimit:
            ok = TransformLimitOp(
                dynamic_cast<const ::fesql::node::LimitPlanNode*>(node), &op,
                status);
            break;
        case node::kPlanTypeProject:
            ok = TransformProjecPlantOp(
                dynamic_cast<const ::fesql::node::ProjectPlanNode*>(node), &op,
                status);
            break;
        case node::kPlanTypeJoin:
            ok = TransformJoinOp(
                dynamic_cast<const ::fesql::node::JoinPlanNode*>(node), &op,
                status);
            break;
        case node::kPlanTypeUnion:
            ok = TransformUnionOp(
                dynamic_cast<const ::fesql::node::UnionPlanNode*>(node), &op,
                status);
            break;
        case node::kPlanTypeGroup:
            ok = TransformGroupOp(
                dynamic_cast<const ::fesql::node::GroupPlanNode*>(node), &op,
                status);
            break;
        case node::kPlanTypeSort:
            ok = TransformSortOp(
                dynamic_cast<const ::fesql::node::SortPlanNode*>(node), &op,
                status);
            break;
        case node::kPlanTypeFilter:
            ok = TransformFilterOp(
                dynamic_cast<const ::fesql::node::FilterPlanNode*>(node), &op,
                status);
            break;
        case node::kPlanTypeTable:
            ok = TransformScanOp(
                dynamic_cast<const ::fesql::node::TablePlanNode*>(node), &op,
                status);
            break;
        case node::kPlanTypeQuery:
            ok = TransformQueryPlan(
                dynamic_cast<const ::fesql::node::QueryPlanNode*>(node), &op,
                status);
            break;
        case node::kPlanTypeRename:
            ok = TransformRenameOp(
                dynamic_cast<const ::fesql::node::RenamePlanNode*>(node), &op,
                status);
            break;
        case node::kPlanTypeDistinct:
            ok = TransformDistinctOp(
                dynamic_cast<const ::fesql::node::DistinctPlanNode*>(node), &op,
                status);
            break;
        default: {
            status.msg = "fail to transform physical plan: can't handle type " +
                         node::NameOfPlanNodeType(node->type_);
            status.code = common::kPlanError;
            LOG(WARNING) << status;
            return false;
        }
    }
    if (!ok) {
        LOG(WARNING) << "fail to tranform physical plan: fail node " +
                            node::NameOfPlanNodeType(node->type_) + ": " +
                            status.str();
        return false;
    }
    op_map_[logical_op] = op;
    *ouput = op;
    return true;
}

bool BatchModeTransformer::GenPlanNode(PhysicalOpNode* node,
                                       base::Status& status) {
    if (nullptr == node) {
        status.msg = "input node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }

    for (auto producer : node->producers()) {
        if (!GenPlanNode(producer, status)) {
            return false;
        }
    }

    if (!node->GetFnName().empty()) {
        DLOG(INFO) << "already gen code for node";
        return true;
    }
    std::string fn_name = "";
    vm::Schema fn_schema;
    switch (node->type_) {
        case kPhysicalOpGroupBy: {
            auto group_op = dynamic_cast<PhysicalGroupNode*>(node);
            if (!GenKey(&group_op->group_,
                        node->producers()[0]->GetOutputNameSchemaList(),
                        status)) {
                return false;
            }
            break;
        }
        case kPhysicalOpSortBy: {
            auto sort_op = dynamic_cast<PhysicalSortNode*>(node);
            if (!GenSort(&sort_op->sort_,
                         node->producers()[0]->GetOutputNameSchemaList(),
                         status)) {
                return false;
            }
            break;
        }
        case kPhysicalOpSimpleProject: {
            auto simple_project =
                dynamic_cast<PhysicalSimpleProjectNode*>(node);
            if (!GenSimpleProject(&simple_project->project_,
                                  simple_project->producers()[0], status)) {
                return false;
            }
            break;
        }
        case kPhysicalOpProject: {
            auto project_op = dynamic_cast<PhysicalProjectNode*>(node);
            if (kWindowAggregation != project_op->project_type_) {
                return true;
            }
            auto window_agg_op =
                dynamic_cast<PhysicalWindowAggrerationNode*>(node);
            if (!GenWindow(&window_agg_op->window_,
                           window_agg_op->producers()[0], status)) {
                return false;
            }
            if (!GenWindowUnionList(&window_agg_op->window_unions_,
                                    window_agg_op->producers()[0], status)) {
                return false;
            }
            if (!GenWindowJoinList(&window_agg_op->window_joins_,
                                   window_agg_op->producers()[0], status)) {
                return false;
            }

            break;
        }
        case kPhysicalOpFilter: {
            auto op = dynamic_cast<PhysicalFliterNode*>(node);
            if (!GenFilter(&op->filter_, node->producers()[0], status)) {
                return false;
            }
            break;
        }
        case kPhysicalOpJoin: {
            auto op = dynamic_cast<PhysicalJoinNode*>(node);
            if (!GenJoin(&op->join_, node, status)) {
                return false;
            }

            break;
        }
        case kPhysicalOpRequestJoin: {
            auto request_join_op = dynamic_cast<PhysicalRequestJoinNode*>(node);
            if (!GenJoin(&request_join_op->join_, node, status)) {
                return false;
            }

            break;
        }
        case kPhysicalOpRequestUnoin: {
            auto request_union_op =
                dynamic_cast<PhysicalRequestUnionNode*>(node);
            if (!GenRequestWindow(&request_union_op->window_,
                                  node->producers()[1], status)) {
                return false;
            }
            if (!GenRequestWindowUnionList(&request_union_op->window_unions_,
                                           request_union_op->producers()[0],
                                           status)) {
                return false;
            }
            break;
        }
        default: {
            return true;
        }
    }
    node->SetFnSchema(fn_schema);
    node->SetFnName(fn_name);
    return true;
}
bool BatchModeTransformer::TransformLimitOp(const node::LimitPlanNode* node,
                                            PhysicalOpNode** output,
                                            base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }
    PhysicalOpNode* depend = nullptr;
    if (!TransformPlanOp(node->GetChildren()[0], &depend, status)) {
        return false;
    }
    *output = new PhysicalLimitNode(depend, node->limit_cnt_);
    node_manager_->RegisterNode(*output);
    return true;
}

bool BatchModeTransformer::TransformProjecPlantOp(
    const node::ProjectPlanNode* node, PhysicalOpNode** output,
    base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }
    PhysicalOpNode* depend = nullptr;
    if (!node->GetChildren().empty() && nullptr != node->GetChildren()[0]) {
        if (!TransformPlanOp(node->GetChildren()[0], &depend, status)) {
            return false;
        }
    }

    if (node->project_list_vec_.empty()) {
        status.msg = "fail transform project op: empty projects";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }

    if (1 == node->project_list_vec_.size()) {
        return TransformProjectOp(dynamic_cast<fesql::node::ProjectListNode*>(
                                      node->project_list_vec_[0]),
                                  depend, output, status);
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
        if (!TransformProjectOp(project_list, depend, &project_op, status)) {
            return false;
        }
        dynamic_cast<PhysicalWindowAggrerationNode*>(project_op)->AppendInput();
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
                for (auto column : depend->output_schema_) {
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
    return TransformProjectOp(project_list, depend, output, status);
}

bool BatchModeTransformer::TransformWindowOp(PhysicalOpNode* depend,
                                             const node::WindowPlanNode* w_ptr,
                                             PhysicalOpNode** output,
                                             base::Status& status) {
    if (nullptr == depend || nullptr == w_ptr || nullptr == output) {
        status.msg = "depend node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }
    if (!CheckHistoryWindowFrame(w_ptr, status)) {
        return false;
    }

    auto check_status = CheckTimeOrIntegerOrderColumn(
        w_ptr->GetOrders(), depend->GetOutputNameSchemaList());
    if (!check_status.isOK()) {
        status = check_status;
        return false;
    }
    const node::OrderByNode* orders = w_ptr->GetOrders();
    const node::ExprListNode* groups = w_ptr->GetKeys();

    switch (depend->type_) {
        case kPhysicalOpDataProvider: {
            auto data_op = dynamic_cast<PhysicalDataProviderNode*>(depend);
            if (kProviderTypeRequest == data_op->provider_type_) {
                auto name = data_op->table_handler_->GetName();
                auto table = catalog_->GetTable(db_, name);
                if (table) {
                    auto right = new PhysicalTableProviderNode(table);
                    node_manager_->RegisterNode(right);

                    auto request_union_op =
                        new PhysicalRequestUnionNode(depend, right, w_ptr);
                    node_manager_->RegisterNode(request_union_op);
                    if (!w_ptr->union_tables().empty()) {
                        for (auto iter = w_ptr->union_tables().cbegin();
                             iter != w_ptr->union_tables().cend(); iter++) {
                            PhysicalOpNode* union_table_op;
                            if (!TransformPlanOp(*iter, &union_table_op,
                                                 status)) {
                                return false;
                            }
                            if (!request_union_op->AddWindowUnion(
                                    union_table_op)) {
                                status.msg =
                                    "Fail to add request window union table";
                                status.code = common::kPlanError;
                                return false;
                            }
                        }
                    }
                    *output = request_union_op;
                    return true;
                } else {
                    status.code = common::kPlanError;
                    status.msg = "fail to transform data provider op: table " +
                                 name + "not exists";
                    LOG(WARNING) << status;
                    return false;
                }
            }
            break;
        }
        case kPhysicalOpRequestJoin: {
            auto join_op = dynamic_cast<PhysicalRequestJoinNode*>(depend);
            switch (join_op->join().join_type()) {
                case node::kJoinTypeLeft:
                case node::kJoinTypeLast: {
                    SchemasContext ctx(depend->GetOutputNameSchemaList());
                    if (!node::ExprListNullOrEmpty(groups)) {
                        const RowSchemaInfo* info;
                        if (!ctx.ExprListResolvedFromSchema(groups->children_,
                                                            &info)) {
                            status.msg =
                                "fail to handle window: group "
                                "expression should belong to left table";
                            LOG(WARNING) << status;
                            return false;
                        }
                        if (0 != info->idx_) {
                            status.msg =
                                "fail to handle window: group "
                                "expression should belong to left table";
                            LOG(WARNING) << status;
                            return false;
                        }
                    }
                    if (nullptr != orders &&
                        !node::ExprListNullOrEmpty(orders->order_by_)) {
                        const RowSchemaInfo* info;
                        if (!ctx.ExprListResolvedFromSchema(
                                orders->order_by_->children_, &info)) {
                            status.msg =
                                "fail to handle window: order "
                                "expression should belong to left table";
                            LOG(WARNING) << status;
                            return false;
                        }
                        if (0 != info->idx_) {
                            status.msg =
                                "fail to handle window: order "
                                "expression should belong to left table";
                            LOG(WARNING) << status;
                            return false;
                        }
                    }

                    auto request_op = dynamic_cast<PhysicalDataProviderNode*>(
                        join_op->producers()[0]);
                    auto name = request_op->table_handler_->GetName();
                    auto table = catalog_->GetTable(db_, name);
                    if (table) {
                        auto right = new PhysicalTableProviderNode(table);
                        node_manager_->RegisterNode(right);
                        auto request_union_op = new PhysicalRequestUnionNode(
                            request_op, right, w_ptr);
                        node_manager_->RegisterNode(request_union_op);
                        if (!w_ptr->union_tables().empty()) {
                            for (auto iter = w_ptr->union_tables().cbegin();
                                 iter != w_ptr->union_tables().cend(); iter++) {
                                PhysicalOpNode* union_table_op;
                                if (!TransformPlanOp(*iter, &union_table_op,
                                                     status)) {
                                    return false;
                                }
                                if (!request_union_op->AddWindowUnion(
                                        union_table_op)) {
                                    status.msg =
                                        "Fail to add request window union "
                                        "table";
                                    status.code = common::kPlanError;
                                    return false;
                                }
                            }
                        }
                        auto join_node = new PhysicalJoinNode(
                            request_union_op, join_op->producers()[1],
                            join_op->join_);
                        node_manager_->RegisterNode(join_node);
                        *output = join_node;
                        return true;
                    } else {
                        status.code = common::kPlanError;
                        status.msg =
                            "fail to transform data provider op: table " +
                            name + "not exists";
                        LOG(WARNING) << status;
                        return false;
                    }
                    break;
                }
                default: {
                    // do nothing
                }
            }
        }
        case kPhysicalOpSimpleProject: {
            auto simple_project =
                dynamic_cast<PhysicalSimpleProjectNode*>(depend);
            if (depend->GetProducer(0)->type_ != kPhysicalOpDataProvider) {
                break;
            }
            auto data_op =
                dynamic_cast<PhysicalDataProviderNode*>(depend->GetProducer(0));
            if (kProviderTypeRequest == data_op->provider_type_) {
                auto name = data_op->table_handler_->GetName();
                auto table = catalog_->GetTable(db_, name);
                if (table) {
                    auto right = new PhysicalTableProviderNode(table);
                    node_manager_->RegisterNode(right);
                    auto right_simple_project = new PhysicalSimpleProjectNode(
                        right, simple_project->output_schema_,
                        simple_project->project_.column_sources_);
                    auto request_union_op = new PhysicalRequestUnionNode(
                        depend, right_simple_project, w_ptr);
                    node_manager_->RegisterNode(request_union_op);
                    if (!w_ptr->union_tables().empty()) {
                        for (auto iter = w_ptr->union_tables().cbegin();
                             iter != w_ptr->union_tables().cend(); iter++) {
                            PhysicalOpNode* union_table_op;
                            if (!TransformPlanOp(*iter, &union_table_op,
                                                 status)) {
                                return false;
                            }
                            if (!request_union_op->AddWindowUnion(
                                    union_table_op)) {
                                status.msg =
                                    "Fail to add request window union table";
                                status.code = common::kPlanError;
                                return false;
                            }
                        }
                    }
                    *output = request_union_op;
                    return true;
                } else {
                    status.code = common::kPlanError;
                    status.msg = "fail to transform data provider op: table " +
                                 name + "not exists";
                    LOG(WARNING) << status;
                    return false;
                }
            }
            break;
        }
        default: {
            // do nothing
        }
    }
    return true;
}
bool BatchModeTransformer::TransformJoinOp(const node::JoinPlanNode* node,
                                           PhysicalOpNode** output,
                                           base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }
    PhysicalOpNode* left = nullptr;
    PhysicalOpNode* right = nullptr;
    if (!TransformPlanOp(node->GetChildren()[0], &left, status)) {
        return false;
    }
    if (!TransformPlanOp(node->GetChildren()[1], &right, status)) {
        return false;
    }

    *output = new PhysicalJoinNode(left, right, node->join_type_, node->orders_,
                                   node->condition_);
    node_manager_->RegisterNode(*output);
    auto check_status = CheckTimeOrIntegerOrderColumn(
        node->orders_, (*output)->GetOutputNameSchemaList());
    if (!check_status.isOK()) {
        status = check_status;
        return false;
    }
    return true;
}
bool BatchModeTransformer::TransformUnionOp(const node::UnionPlanNode* node,
                                            PhysicalOpNode** output,
                                            base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }
    PhysicalOpNode* left = nullptr;
    PhysicalOpNode* right = nullptr;
    if (!TransformPlanOp(node->GetChildren()[0], &left, status)) {
        return false;
    }
    if (!TransformPlanOp(node->GetChildren()[1], &right, status)) {
        return false;
    }
    *output = new PhysicalUnionNode(left, right, node->is_all);
    node_manager_->RegisterNode(*output);
    return true;
}
bool BatchModeTransformer::TransformGroupOp(const node::GroupPlanNode* node,
                                            PhysicalOpNode** output,
                                            base::Status& status) {
    PhysicalOpNode* left = nullptr;
    if (!TransformPlanOp(node->GetChildren()[0], &left, status)) {
        return false;
    }

    if (kPhysicalOpDataProvider == left->type_) {
        auto data_op = dynamic_cast<PhysicalDataProviderNode*>(left);
        if (kProviderTypeRequest == data_op->provider_type_) {
            auto name = data_op->table_handler_->GetName();
            auto table = catalog_->GetTable(db_, name);
            if (table) {
                auto right = new PhysicalTableProviderNode(table);
                node_manager_->RegisterNode(right);
                *output =
                    new PhysicalRequestUnionNode(left, right, node->by_list_);
                node_manager_->RegisterNode(*output);
                return true;
            } else {
                status.code = common::kPlanError;
                status.msg = "fail to transform data provider op: table " +
                             name + "not exists";
                LOG(WARNING) << status;
                return false;
            }
        }
    }

    *output = new PhysicalGroupNode(left, node->by_list_);
    node_manager_->RegisterNode(*output);
    return true;
}
bool BatchModeTransformer::TransformSortOp(const node::SortPlanNode* node,
                                           PhysicalOpNode** output,
                                           base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }
    PhysicalOpNode* left = nullptr;
    if (!TransformPlanOp(node->GetChildren()[0], &left, status)) {
        return false;
    }
    *output = new PhysicalSortNode(left, node->order_list_);
    node_manager_->RegisterNode(*output);
    return true;
}
bool BatchModeTransformer::TransformFilterOp(const node::FilterPlanNode* node,
                                             PhysicalOpNode** output,
                                             base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }
    PhysicalOpNode* depend = nullptr;
    if (!TransformPlanOp(node->GetChildren()[0], &depend, status)) {
        return false;
    }
    *output = new PhysicalFliterNode(depend, node->condition_);
    node_manager_->RegisterNode(*output);
    return true;
}

bool BatchModeTransformer::TransformScanOp(const node::TablePlanNode* node,
                                           PhysicalOpNode** output,
                                           base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }
    auto table = catalog_->GetTable(db_, node->table_);
    if (table) {
        *output = new PhysicalTableProviderNode(table);
        node_manager_->RegisterNode(*output);
    } else {
        status.msg = "fail to transform data_provider op: table " + db_ + "." +
                     node->table_ + " not exist!";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }
    return true;
}

bool BatchModeTransformer::TransformRenameOp(const node::RenamePlanNode* node,
                                             PhysicalOpNode** output,
                                             base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        return false;
    }
    PhysicalOpNode* left = nullptr;
    if (!TransformPlanOp(node->GetChildren()[0], &left, status)) {
        return false;
    }
    vm::SchemaSourceList new_sources;

    for (auto source : left->GetOutputNameSchemaList().schema_source_list_) {
        new_sources.AddSchemaSource(node->table_, source.schema_,
                                    source.sources_);
    }
    left->SetOutputNameSchemaList(new_sources);
    *output = left;
    return true;
}

bool BatchModeTransformer::TransformDistinctOp(
    const node::DistinctPlanNode* node, PhysicalOpNode** output,
    base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        return false;
    }
    PhysicalOpNode* left = nullptr;
    if (!TransformPlanOp(node->GetChildren()[0], &left, status)) {
        return false;
    }
    *output = new PhysicalDistinctNode(left);
    node_manager_->RegisterNode(*output);
    return true;
}
bool BatchModeTransformer::TransformQueryPlan(
    const ::fesql::node::PlanNode* node, ::fesql::vm::PhysicalOpNode** output,
    ::fesql::base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        return false;
    }
    if (!TransformPlanOp(node->GetChildren()[0], output, status)) {
        return false;
    }
    return true;
}
bool BatchModeTransformer::AddPass(PhysicalPlanPassType type) {
    passes.push_back(type);
    return true;
}

fesql::base::Status ResolveProjects(
    const SchemaSourceList& input_schemas, const node::PlanNodeList& projects,
    const bool row_project, node::NodeManager* node_manager,
    udf::UDFLibrary* library, node::LambdaNode** output_func,
    std::vector<std::string>* output_names,
    std::vector<node::FrameNode*>* output_frames) {
    // lambdafy
    const bool enable_legacy_agg_opt = true;
    passes::LambdafyProjects lambdafy_pass(node_manager, library, input_schemas,
                                           enable_legacy_agg_opt);
    node::LambdaNode* lambda = nullptr;
    std::vector<int> require_agg;
    CHECK_STATUS(lambdafy_pass.Transform(projects, &lambda, &require_agg,
                                         output_names, output_frames));

    // check lambdafy output
    CHECK_TRUE(lambda->GetArgSize() == 2, kCodegenError);
    std::vector<const node::TypeNode*> global_arg_types = {
        lambda->GetArgType(0), lambda->GetArgType(1)};
    if (row_project) {
        for (size_t i = 0; i < require_agg.size(); ++i) {
            CHECK_TRUE(require_agg[i] == false, kCodegenError,
                       "Can not gen agg project in row project node, ", i,
                       "th expression is: ",
                       lambda->body()->GetChild(i)->GetExprString());
        }
    }

    // type inference and udf function resolve
    node::LambdaNode* resolved_lambda = nullptr;
    passes::ResolveFnAndAttrs resolve_pass(node_manager, library,
                                           vm::SchemasContext(input_schemas));
    CHECK_STATUS(
        resolve_pass.VisitLambda(lambda, global_arg_types, &resolved_lambda));

    *output_func = resolved_lambda;
    return fesql::base::Status::OK();
}

bool BatchModeTransformer::GenProjects(const SchemaSourceList& input_schemas,
                                       const node::PlanNodeList& projects,
                                       const bool row_project,
                                       const node::FrameNode* frame,
                                       std::string& fn_name,   // NOLINT
                                       Schema* output_schema,  // NOLINT
                                       ColumnSourceList* output_column_sources,
                                       base::Status& status) {  // NOLINT
    // run expr passes
    node::LambdaNode* project_func = nullptr;
    std::vector<std::string> project_names;
    std::vector<node::FrameNode*> project_frames;
    status = ResolveProjects(input_schemas, projects, row_project,
                             node_manager_, library_, &project_func,
                             &project_names, &project_frames);
    if (!status.isOK()) {
        LOG(WARNING) << status;
        return false;
    }

    // TODO(wangtaize) use ops end op output schema
    vm::SchemasContext schemas_context(input_schemas);
    codegen::CodeGenContext codegen_ctx(module_, &schemas_context,
                                        node_manager_);
    codegen::RowFnLetIRBuilder builder(&codegen_ctx, frame);
    fn_name = "__internal_sql_codegen_" + std::to_string(id_++);
    status = builder.Build(fn_name, project_func, project_names, project_frames,
                           output_schema, output_column_sources);
    return status.isOK();
}

bool BatchModeTransformer::AddDefaultPasses() {
    AddPass(kPassColumnProjectOptimized);
    AddPass(kPassFilterOptimized);
    AddPass(kPassLeftJoinOptimized);
    AddPass(kPassGroupAndSortOptimized);
    AddPass(kPassLimitOptimized);
    return false;
}

bool BatchModeTransformer::CreatePhysicalConstProjectNode(
    node::ProjectListNode* project_list, PhysicalOpNode** output,
    base::Status& status) {
    if (nullptr == project_list || nullptr == output) {
        status.msg = "project list node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }
    const node::PlanNodeList& projects = project_list->GetProjects();
    node::PlanNodeList new_projects;

    for (auto iter = projects.cbegin(); iter != projects.cend(); iter++) {
        auto project_node = dynamic_cast<node::ProjectNode*>(*iter);
        auto expr = project_node->GetExpression();
        if (nullptr == expr) {
            status.msg = "invalid project: expression is null";
            status.code = common::kPlanError;
            return false;
        }
        if (node::kExprAll == expr->expr_type_) {
            status.msg = "invalid project: no table used";
            status.code = common::kPlanError;
            return false;
        }
    }
    Schema output_schema;
    ColumnSourceList output_column_sources;
    std::string fn_name;
    vm::SchemaSourceList empty_schema_source;
    if (!GenProjects(empty_schema_source, projects, true, nullptr, fn_name,
                     &output_schema, &output_column_sources, status)) {
        return false;
    }
    PhysicalOpNode* op = new PhysicalConstProjectNode(fn_name, output_schema,
                                                      output_column_sources);
    node_manager_->RegisterNode(op);
    *output = op;
    return true;
}
bool BatchModeTransformer::CreatePhysicalProjectNode(
    const ProjectType project_type, PhysicalOpNode* node,
    node::ProjectListNode* project_list, PhysicalOpNode** output,
    base::Status& status) {
    if (nullptr == project_list || nullptr == output) {
        status.msg = "project list node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }
    const node::PlanNodeList& projects = project_list->GetProjects();

    node::PlanNodeList new_projects;
    bool has_all_project = false;

    for (auto iter = projects.cbegin(); iter != projects.cend(); iter++) {
        auto project_node = dynamic_cast<node::ProjectNode*>(*iter);
        auto expr = project_node->GetExpression();
        if (nullptr == expr) {
            status.msg = "invalid project: expression is null";
            status.code = common::kPlanError;
            return false;
        }
        if (node::kExprAll == expr->expr_type_) {
            auto all_expr = dynamic_cast<node::AllNode*>(expr);
            if (all_expr->children_.empty()) {
                // expand all expression if needed
                for (auto column : node->output_schema_) {
                    all_expr->children_.push_back(
                        node_manager_->MakeColumnRefNode(
                            column.name(), all_expr->GetRelationName()));
                }
            }
            has_all_project = true;
        }
    }

    if (has_all_project && 1 == projects.size() &&
        node->GetOutputSchemaListSize() == 1) {
        // skip project
        DLOG(INFO) << "skip project node: project has only kAllExpr "
                      "expression";
        *output = node;
        return true;
    }

    Schema output_schema;
    ColumnSourceList output_column_sources;
    std::string fn_name;
    switch (project_type) {
        case kRowProject:
        case kTableProject: {
            if (!GenProjects((node->GetOutputNameSchemaList()), projects, true,
                             nullptr, fn_name, &output_schema,
                             &output_column_sources, status)) {
                return false;
            }
            break;
        }
        case kAggregation:
        case kGroupAggregation:
        case kWindowAggregation: {
            // TODO(chenjing): gen window aggregation
            if (!GenProjects((node->GetOutputNameSchemaList()), projects, false,
                             nullptr == project_list->GetW()
                                 ? nullptr
                                 : project_list->GetW()->frame_node(),
                             fn_name, &output_schema, &output_column_sources,
                             status)) {
                return false;
            }
            break;
        }
    }

    PhysicalOpNode* op = nullptr;
    switch (project_type) {
        case kRowProject: {
            if (IsSimpleProject(output_column_sources)) {
                op = new PhysicalSimpleProjectNode(node, output_schema,
                                                   output_column_sources);
            } else {
                op = new PhysicalRowProjectNode(node, fn_name, output_schema,
                                                output_column_sources);
            }
            break;
        }
        case kTableProject: {
            if (IsSimpleProject(output_column_sources)) {
                op = new PhysicalSimpleProjectNode(node, output_schema,
                                                   output_column_sources);
            } else {
                op = new PhysicalTableProjectNode(node, fn_name, output_schema,
                                                  output_column_sources);
            }
            break;
        }
        case kAggregation: {
            op = new PhysicalAggrerationNode(node, fn_name, output_schema,
                                             output_column_sources);
            break;
        }
        case kGroupAggregation: {
            auto group_op = dynamic_cast<PhysicalGroupNode*>(node);
            op = new PhysicalGroupAggrerationNode(node, group_op->group_.keys(),
                                                  fn_name, output_schema,
                                                  output_column_sources);
            break;
        }
        case kWindowAggregation: {
            auto window_agg_op = new PhysicalWindowAggrerationNode(
                node, project_list->w_ptr_, fn_name, output_schema,
                output_column_sources);

            if (!project_list->w_ptr_->union_tables().empty()) {
                for (auto iter = project_list->w_ptr_->union_tables().cbegin();
                     iter != project_list->w_ptr_->union_tables().cend();
                     iter++) {
                    PhysicalOpNode* union_table_op;
                    if (!TransformPlanOp(*iter, &union_table_op, status)) {
                        return false;
                    }
                    if (!window_agg_op->AddWindowUnion(union_table_op)) {
                        status.msg = "Fail to add window union table";
                        status.code = common::kPlanError;
                        return false;
                    }
                }
            }
            op = window_agg_op;
            break;
        }
    }
    node_manager_->RegisterNode(op);
    *output = op;
    return true;
}

bool BatchModeTransformer::TransformProjectOp(
    node::ProjectListNode* project_list, PhysicalOpNode* node,
    PhysicalOpNode** output, base::Status& status) {
    auto depend = node;
    if (nullptr == depend) {
        return CreatePhysicalConstProjectNode(project_list, output, status);
    }
    switch (depend->output_type_) {
        case kSchemaTypeRow:
            return CreatePhysicalProjectNode(kRowProject, depend, project_list,
                                             output, status);
        case kSchemaTypeGroup:
            return CreatePhysicalProjectNode(kGroupAggregation, depend,
                                             project_list, output, status);
        case kSchemaTypeTable:
            if (project_list->is_window_agg_) {
                if (!CheckHistoryWindowFrame(project_list->w_ptr_, status)) {
                    return false;
                }

                auto check_status = CheckTimeOrIntegerOrderColumn(
                    project_list->w_ptr_->GetOrders(),
                    depend->GetOutputNameSchemaList());
                if (!check_status.isOK()) {
                    status = check_status;
                    return false;
                }
                return CreatePhysicalProjectNode(kWindowAggregation, depend,
                                                 project_list, output, status);
            } else {
                return CreatePhysicalProjectNode(kTableProject, depend,
                                                 project_list, output, status);
            }
    }
    return false;
}
void BatchModeTransformer::ApplyPasses(PhysicalOpNode* node,
                                       PhysicalOpNode** output) {
    auto physical_plan = node;
    for (auto type : passes) {
        switch (type) {
            case kPassColumnProjectOptimized: {
                SimpleProjectOptimized pass(node_manager_, db_, catalog_);
                PhysicalOpNode* new_op = nullptr;
                if (pass.Apply(physical_plan, &new_op)) {
                    physical_plan = new_op;
                }
                break;
            }

            case kPassFilterOptimized: {
                ConditionOptimized pass(node_manager_, db_, catalog_);
                PhysicalOpNode* new_op = nullptr;
                if (pass.Apply(physical_plan, &new_op)) {
                    physical_plan = new_op;
                }
                break;
            }
            case kPassGroupAndSortOptimized: {
                GroupAndSortOptimized pass(node_manager_, db_, catalog_);
                PhysicalOpNode* new_op = nullptr;
                if (pass.Apply(physical_plan, &new_op)) {
                    physical_plan = new_op;
                }
                break;
            }
            case kPassLeftJoinOptimized: {
                if (catalog_->IndexSupport()) {
                    LeftJoinOptimized pass(node_manager_, db_, catalog_);
                    PhysicalOpNode* new_op = nullptr;
                    if (pass.Apply(physical_plan, &new_op)) {
                        physical_plan = new_op;
                    }
                }
                break;
            }
            case kPassLimitOptimized: {
                LimitOptimized pass(node_manager_, db_, catalog_);
                PhysicalOpNode* new_op = nullptr;
                if (pass.Apply(physical_plan, &new_op)) {
                    physical_plan = new_op;
                }
                break;
            }
            default: {
                LOG(WARNING) << "can't not handle pass: "
                             << PhysicalPlanPassTypeName(type);
            }
        }
    }
    *output = physical_plan;
}
base::Status BatchModeTransformer::ValidatePartitionDataProvider(
    PhysicalOpNode* in) {
    CHECK_TRUE(nullptr != in, kPlanError, "Invalid physical node: null");
    if (kPhysicalOpSimpleProject == in->type_) {
        CHECK_STATUS(ValidatePartitionDataProvider(in->GetProducer(0)))
    } else {
        CHECK_TRUE(
            kPhysicalOpDataProvider == in->type_ &&
                kProviderTypePartition ==
                    dynamic_cast<PhysicalDataProviderNode*>(in)->provider_type_,
            kPlanError, "Isn't partition provider");
    }
    return base::Status::OK();
}

base::Status BatchModeTransformer::ValidateJoinIndexOptimization(
    const Join& join, PhysicalOpNode* right) {
    CHECK_TRUE(nullptr != right, kPlanError, "Invalid physical node: null");
    if (node::kJoinTypeConcat == join.join_type_) {
        return base::Status::OK();
    }

    if (node::kJoinTypeLast == join.join_type_) {
        CHECK_TRUE(
            nullptr == join.right_sort_.orders() ||
                node::ExprListNullOrEmpty(join.right_sort_.orders()->order_by_),
            kPlanError, "Last Join node order by hasn't been optimized")
    }
    if (kSchemaTypeRow == right->output_type_) {
        // skip index optimization check when join a row
        return base::Status::OK();
    } else {
        CHECK_STATUS(ValidatePartitionDataProvider(right),
                     "Join node hasn't been optimized");
    }

    return base::Status::OK();
}
base::Status BatchModeTransformer::ValidateWindowIndexOptimization(
    const WindowOp& window, PhysicalOpNode* in) {
    CHECK_TRUE(nullptr != in, kPlanError, "Invalid physical node: null");
    CHECK_STATUS(ValidatePartitionDataProvider(in),
                 "Window node hasn't been optimized");
    // check if window's order expression has been optimized
    CHECK_TRUE(!window.sort().ValidSort() ||
                   node::ExprListNullOrEmpty(window.sort().orders()->order_by_),
               kPlanError, "Window node hasn't been optimzied")
    return base::Status::OK();
}
base::Status BatchModeTransformer::ValidateIndexOptimization(
    PhysicalOpNode* in) {
    CHECK_TRUE(nullptr != in, kPlanError, "Invalid physical node: null");

    switch (in->type_) {
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
        case kPhysicalOpRequestUnoin: {
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
            PhysicalFliterNode* filter_op =
                dynamic_cast<PhysicalFliterNode*>(in);
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
    return base::Status::OK();
}
bool BatchModeTransformer::TransformPhysicalPlan(
    const ::fesql::node::PlanNodeList& trees,
    ::fesql::vm::PhysicalOpNode** output, base::Status& status) {
    if (nullptr == module_ || trees.empty()) {
        status.msg = "module or logical trees is empty";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }

    auto it = trees.begin();
    for (; it != trees.end(); ++it) {
        const ::fesql::node::PlanNode* node = *it;
        switch (node->GetType()) {
            case ::fesql::node::kPlanTypeFuncDef: {
                const ::fesql::node::FuncDefPlanNode* func_def_plan =
                    dynamic_cast<const ::fesql::node::FuncDefPlanNode*>(node);
                if (!GenFnDef(func_def_plan, status)) {
                    return false;
                }
                break;
            }
            case ::fesql::node::kPlanTypeUnion:
            case ::fesql::node::kPlanTypeQuery: {
                PhysicalOpNode* physical_plan = nullptr;
                if (!TransformQueryPlan(node, &physical_plan, status)) {
                    LOG(WARNING)
                        << "fail to transform query plan to physical plan";
                    return false;
                }
                ApplyPasses(physical_plan, output);

                if (performance_sensitive_mode_ &&
                    !ValidateIndexOptimization(physical_plan).isOK()) {
                    LOG(WARNING) << "fail to support physical plan in "
                                    "performance sensitive mode";
                    std::ostringstream oss;
                    physical_plan->Print(oss, "");
                    LOG(INFO) << "physical plan:\n" << oss.str() << std::endl;
                    return false;
                }
                if (!GenPlanNode(*output, status)) {
                    LOG(WARNING) << "fail to gen plan";
                    return false;
                }
                break;
            }
            default: {
                LOG(WARNING) << "not supported";
                return false;
            }
        }
    }
    return true;
}
bool BatchModeTransformer::GenFnDef(const node::FuncDefPlanNode* fn_plan,
                                    base::Status& status) {
    if (nullptr == module_ || nullptr == fn_plan ||
        nullptr == fn_plan->fn_def_) {
        status.msg = "fail to codegen function: module or fn_def node is null";
        status.code = common::kOpGenError;
        LOG(WARNING) << status;
        return false;
    }

    ::fesql::codegen::FnIRBuilder builder(module_);
    ::llvm::Function* fn = nullptr;
    bool ok = builder.Build(fn_plan->fn_def_, &fn, status);
    if (!ok) {
        status.msg = "fail to codegen function: " + status.str();
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }
    return true;
}

bool BatchModeTransformer::CodeGenExprList(
    const SchemaSourceList& input_name_schema_list,
    const node::ExprListNode* expr_list, bool row_mode, FnInfo* fn_info,
    base::Status& status) {
    if (node::ExprListNullOrEmpty(expr_list)) {
        status.msg = "fail to codegen expr list: null or empty list";
        status.code = common::kCodegenError;
        return false;
    }
    if (!fn_info->fn_name().empty()) {
        DLOG(INFO) << "codegen already " << fn_info->fn_name();
        return true;
    }
    node::PlanNodeList projects;
    int32_t pos = 0;
    for (auto expr : expr_list->children_) {
        projects.push_back(node_manager_->MakeRowProjectNode(
            pos++, node::ExprString(expr), expr));
    }
    vm::ColumnSourceList column_sources;
    return GenProjects(input_name_schema_list, projects, true, nullptr,
                       fn_info->fn_name_, &fn_info->fn_schema_, &column_sources,
                       status);
}

bool BatchModeTransformer::GenJoin(Join* join, PhysicalOpNode* in,
                                   base::Status& status) {
    if (!GenConditionFilter(&join->condition_, in->GetOutputNameSchemaList(),
                            status)) {
        return false;
    }
    if (!GenKey(&join->left_key_, in->producers()[0]->GetOutputNameSchemaList(),
                status)) {
        return false;
    }
    if (!GenKey(&join->index_key_,
                in->producers()[0]->GetOutputNameSchemaList(), status)) {
        return false;
    }
    if (!GenKey(&join->right_key_,
                in->producers()[1]->GetOutputNameSchemaList(), status)) {
        return false;
    }

    if (join->join_type_ == node::kJoinTypeLast) {
        if (!GenSort(&join->right_sort_,
                     in->producers()[1]->GetOutputNameSchemaList(), status)) {
            return false;
        }
    }

    return true;
}
bool BatchModeTransformer::GenFilter(Filter* filter, PhysicalOpNode* in,
                                     base::Status& status) {
    if (!GenConditionFilter(&filter->condition_, in->GetOutputNameSchemaList(),
                            status)) {
        return false;
    }
    if (!GenKey(&filter->left_key_, in->GetOutputNameSchemaList(), status)) {
        return false;
    }
    if (!GenKey(&filter->index_key_, in->GetOutputNameSchemaList(), status)) {
        return false;
    }
    if (!GenKey(&filter->right_key_, in->GetOutputNameSchemaList(), status)) {
        return false;
    }
    return true;
}
bool BatchModeTransformer::GenConditionFilter(
    ConditionFilter* filter, const SchemaSourceList& input_name_schema_list,
    base::Status& status) {
    if (nullptr != filter->condition_) {
        node::ExprListNode expr_list;
        expr_list.AddChild(const_cast<node::ExprNode*>(filter->condition_));
        if (!CodeGenExprList(input_name_schema_list, &expr_list, true,
                             &filter->fn_info_, status)) {
            return false;
        }
    }
    return true;
}
bool BatchModeTransformer::GenKey(
    Key* hash, const SchemaSourceList& input_name_schema_list,
    base::Status& status) {
    // Gen left key function
    node::ExprListNode expr_list;
    if (!node::ExprListNullOrEmpty(hash->keys_)) {
        for (auto expr : hash->keys_->children_) {
            expr_list.AddChild(expr);
        }
    }
    if (!expr_list.children_.empty()) {
        // Gen left table key
        if (!CodeGenExprList(input_name_schema_list, &expr_list, true,
                             &hash->fn_info_, status)) {
            return false;
        }
    }
    return true;
}
bool BatchModeTransformer::GenWindow(WindowOp* window, PhysicalOpNode* in,
                                     base::Status& status) {
    node::ExprListNode expr_list;
    if (!GenKey(&window->partition_, in->GetOutputNameSchemaList(), status)) {
        return false;
    }

    if (!GenSort(&window->sort_, in->GetOutputNameSchemaList(), status)) {
        return false;
    }

    if (!GenRange(&window->range_, in->GetOutputNameSchemaList(), status)) {
        return false;
    }
    return true;
}
bool BatchModeTransformer::GenRequestWindow(RequestWindowOp* window,
                                            PhysicalOpNode* in,
                                            base::Status& status) {
    node::ExprListNode expr_list;
    if (!GenWindow(window, in, status)) {
        return false;
    }

    if (!GenKey(&window->index_key_, in->GetOutputNameSchemaList(), status)) {
        return false;
    }
    return true;
}

bool BatchModeTransformer::GenSort(
    Sort* sort, const SchemaSourceList& input_name_schema_list,
    base::Status& status) {
    if (nullptr != sort->orders_ &&
        !node::ExprListNullOrEmpty(sort->orders_->order_by_)) {
        node::ExprListNode expr_list;
        for (auto expr : sort->orders_->order_by_->children_) {
            expr_list.AddChild(expr);
        }
        if (!CodeGenExprList(input_name_schema_list, &expr_list, true,
                             &sort->fn_info_, status)) {
            return false;
        }
    }
    return true;
}

bool BatchModeTransformer::GenRange(
    Range* range, const SchemaSourceList& input_name_schema_list,
    base::Status& status) {
    if (nullptr != range->range_key_) {
        node::ExprListNode expr_list;
        expr_list.AddChild(const_cast<node::ExprNode*>(range->range_key_));
        if (!CodeGenExprList(input_name_schema_list, &expr_list, true,
                             &range->fn_info_, status)) {
            return false;
        }
    }
    return true;
}
bool BatchModeTransformer::GenSimpleProject(ColumnProject* project,
                                            PhysicalOpNode* in,
                                            base::Status& status) {
    if (!project->column_sources().empty()) {
        auto schema_souces = in->GetOutputNameSchemaList();
        node::ExprListNode* expr_list =
            node_manager_->BuildExprListFromSchemaSource(
                project->column_sources(), schema_souces);
        if (nullptr == expr_list) {
            LOG(WARNING) << "Fail to Build Expr List";
            return false;
        }
        if (!CodeGenExprList(schema_souces, expr_list, true, &project->fn_info_,
                             status)) {
            return false;
        }
    }
    DLOG(INFO) << "GenSimpleProject:\n" << project->FnDetail();
    return true;
}
bool BatchModeTransformer::GenWindowUnionList(
    WindowUnionList* window_union_list, PhysicalOpNode* in,
    base::Status& status) {
    if (nullptr == window_union_list || window_union_list->Empty()) {
        LOG(WARNING) << "Skip GenWindowUnionList when window unions is empty";
        return true;
    }
    for (auto& window_union : window_union_list->window_unions_) {
        if (!GenPlanNode(window_union.first, status)) {
            LOG(WARNING) << "Fail Gen Window Union Sub Query Plan" << status;
            return false;
        }
        if (!GenWindow(&window_union.second, in, status)) {
            return false;
        }
    }
    DLOG(INFO) << "GenWindowUnionList:\n" << window_union_list->FnDetail();
    return true;
}
bool BatchModeTransformer::GenWindowJoinList(WindowJoinList* window_join_list,
                                             PhysicalOpNode* in,
                                             base::Status& status) {
    if (nullptr != window_join_list && !window_join_list->Empty()) {
        SchemaSourceList joined_schema;
        joined_schema.AddSchemaSources(in->GetOutputNameSchemaList());
        for (auto& window_join : window_join_list->window_joins_) {
            if (!GenPlanNode(window_join.first, status)) {
                LOG(WARNING) << "Fail Gen Window Join Sub Query Plan" << status;
                return false;
            }
            auto right_schema = window_join.first->GetOutputNameSchemaList();
            auto& left_schema = joined_schema;
            auto join = &window_join.second;
            if (!GenKey(&join->left_key_, left_schema, status)) {
                return false;
            }
            if (!GenKey(&join->index_key_, left_schema, status)) {
                return false;
            }
            if (!GenKey(&join->right_key_, right_schema, status)) {
                return false;
            }
            joined_schema.AddSchemaSources(right_schema);
            if (!GenConditionFilter(&join->condition_, joined_schema, status)) {
                return false;
            }
        }
    }
    DLOG(INFO) << "GenWindowJoinList:\n" << window_join_list->FnDetail();
    return true;
}
bool BatchModeTransformer::GenRequestWindowUnionList(
    RequestWindowUnionList* window_union_list, PhysicalOpNode* in,
    base::Status& status) {
    if (nullptr == window_union_list || window_union_list->Empty()) {
        LOG(WARNING)
            << "Skip GenRequestWindowUnionList when window unions is empty";
        return true;
    }
    for (auto& window_union : window_union_list->window_unions_) {
        if (!GenPlanNode(window_union.first, status)) {
            LOG(WARNING) << "Fail Gen Request Window Union Sub Query Plan"
                         << status;
            return false;
        }
        if (!GenRequestWindow(&window_union.second, in, status)) {
            return false;
        }
    }
    DLOG(INFO) << "GenRequestWindowUnionList:\n"
               << window_union_list->FnDetail();
    return true;
}
bool BatchModeTransformer::IsSimpleProject(const ColumnSourceList& sources) {
    bool flag = true;
    for (auto iter = sources.cbegin(); iter != sources.cend(); iter++) {
        if (vm::kSourceNone == iter->type()) {
            flag = false;
            break;
        }
    }
    return flag;
}
base::Status BatchModeTransformer::CheckTimeOrIntegerOrderColumn(
    const node::OrderByNode* orders,
    const vm::SchemaSourceList& schema_source_list) {
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

        SchemasContext ctx(schema_source_list);
        fesql::type::Type type;
        CHECK_STATUS(ctx.ColumnTypeResolved(
            dynamic_cast<node::ColumnRefNode*>(order)->GetRelationName(),
            dynamic_cast<node::ColumnRefNode*>(order)->GetColumnName(), &type));
        switch (type) {
            case type::kInt16:
            case type::kInt32:
            case type::kInt64:
            case type::kTimestamp: {
                return base::Status();
            }
            default: {
                return base::Status(common::kPlanError,
                                    "Invalid Order column type : " +
                                        fesql::type::Type_Name(type));
            }
        }
    }
    return base::Status();
}
bool BatchModeTransformer::CheckHistoryWindowFrame(
    const node::WindowPlanNode* w_ptr, base::Status& status) {
    if (nullptr == w_ptr) {
        status.code = common::kPlanError;
        status.msg = "Invalid Request Window: null window";
        LOG(WARNING) << status;
        return false;
    }
    const node::FrameNode* frame = w_ptr->frame_node();
    if (frame->GetHistoryRangeEnd() > 0 && frame->GetHistoryRowsEnd() > 0) {
        status.code = common::kPlanError;
        status.msg =
            "Invalid Request Window: end frame can't exceed "
            "CURRENT";
        LOG(WARNING) << status;
        return false;
    }
    if (frame->GetHistoryRangeStart() > 0 || frame->GetHistoryRowsStart() > 0) {
        status.code = common::kPlanError;
        status.msg =
            "Invalid Request Window: start frame can't exceed "
            "CURRENT";
        LOG(WARNING) << status;
        return false;
    }
    return true;
}

bool GroupAndSortOptimized::KeysFilterOptimized(
    const vm::SchemaSourceList& column_sources, PhysicalOpNode* in, Key* group,
    Key* hash, PhysicalOpNode** new_in) {
    if (nullptr == group || nullptr == hash) {
        LOG(WARNING) << "Fail KeysFilterOptimized when window filter key or "
                        "index key is empty";
        return false;
    }
    if (kPhysicalOpDataProvider == in->type_) {
        auto scan_op = dynamic_cast<PhysicalDataProviderNode*>(in);
        if (kProviderTypeTable == scan_op->provider_type_) {
            std::string index_name;
            const node::ExprListNode* new_groups = nullptr;
            const node::ExprListNode* keys = nullptr;
            auto& index_hint = scan_op->table_handler_->GetIndex();
            if (!TransformGroupExpr(column_sources, group->keys(), index_hint,
                                    &index_name, &keys, &new_groups)) {
                return false;
            }
            PhysicalPartitionProviderNode* scan_index_op =
                new PhysicalPartitionProviderNode(scan_op, index_name);
            group->set_keys(new_groups);
            hash->set_keys(keys);
            *new_in = scan_index_op;
            return true;
        }
    } else if (kPhysicalOpSimpleProject == in->type_) {
        auto simple_project = dynamic_cast<PhysicalSimpleProjectNode*>(in);
        PhysicalOpNode* new_depend;
        if (!KeysFilterOptimized(simple_project->GetOutputNameSchemaList(),
                                 simple_project->producers()[0], group, hash,
                                 &new_depend)) {
            return false;
        }
        PhysicalSimpleProjectNode* new_simple_op =
            new PhysicalSimpleProjectNode(
                new_depend, simple_project->output_schema_,
                simple_project->project_.column_sources());
        *new_in = new_simple_op;
        return true;
    }
    return false;
}
bool GroupAndSortOptimized::JoinKeysOptimized(
    const vm::SchemaSourceList& column_sources, PhysicalOpNode* in, Join* join,
    PhysicalOpNode** new_in) {
    if (nullptr == join) {
        return false;
    }
    return FilterOptimized(column_sources, in, join, new_in);
}
bool GroupAndSortOptimized::FilterOptimized(
    const vm::SchemaSourceList& column_sources, PhysicalOpNode* in,
    Filter* filter, PhysicalOpNode** new_in) {
    if (nullptr == filter ||
        node::ExprListNullOrEmpty(filter->right_key_.keys())) {
        return false;
    }

    if (kPhysicalOpDataProvider == in->type_) {
        auto scan_op = dynamic_cast<PhysicalDataProviderNode*>(in);
        if (kProviderTypeTable == scan_op->provider_type_) {
            const node::ExprListNode* new_right_partition = nullptr;
            const node::ExprListNode* index_keys = nullptr;
            node::ExprListNode* left_index_keys = nullptr;
            node::ExprListNode* new_left_keys = nullptr;
            std::string index_name;
            auto& index_hint = scan_op->table_handler_->GetIndex();
            const node::ExprListNode* right_partition =
                filter->right_key_.keys();
            if (!TransformGroupExpr(column_sources, right_partition, index_hint,
                                    &index_name, &index_keys,
                                    &new_right_partition)) {
                return false;
            }
            PhysicalPartitionProviderNode* partition_op =
                new PhysicalPartitionProviderNode(scan_op, index_name);
            node_manager_->RegisterNode(partition_op);
            *new_in = partition_op;
            left_index_keys = node_manager_->MakeExprList();
            new_left_keys = node_manager_->MakeExprList();
            for (size_t i = 0; i < right_partition->children_.size(); i++) {
                bool is_index_key = false;
                for (auto key : index_keys->children_) {
                    if (node::ExprEquals(right_partition->children_[i], key)) {
                        is_index_key = true;
                    }
                }
                if (is_index_key) {
                    left_index_keys->AddChild(
                        filter->left_key_.keys_->children_[i]);
                } else {
                    new_left_keys->AddChild(
                        filter->left_key_.keys_->children_[i]);
                }
            }
            filter->right_key_.set_keys(new_right_partition);
            filter->index_key_.set_keys(left_index_keys);
            filter->left_key_.set_keys(new_left_keys);
            return true;
        }
    } else if (kPhysicalOpSimpleProject == in->type_) {
        auto simple_project = dynamic_cast<PhysicalSimpleProjectNode*>(in);
        PhysicalOpNode* new_depend;
        if (!FilterOptimized(simple_project->GetOutputNameSchemaList(),
                             simple_project->producers()[0], filter,
                             &new_depend)) {
            return false;
        }
        PhysicalSimpleProjectNode* new_simple_op =
            new PhysicalSimpleProjectNode(
                new_depend, simple_project->output_schema_,
                simple_project->project_.column_sources());
        new_simple_op->SetOutputNameSchemaList(
            simple_project->GetOutputNameSchemaList());
        *new_in = new_simple_op;
        return true;
    }
    return false;
}

bool GroupAndSortOptimized::GroupOptimized(
    const vm::SchemaSourceList& schema_sources, PhysicalOpNode* in, Key* group,
    PhysicalOpNode** new_in) {
    if (nullptr == group) {
        return false;
    }

    if (kPhysicalOpDataProvider == in->type_) {
        auto scan_op = dynamic_cast<PhysicalDataProviderNode*>(in);
        if (kProviderTypeTable == scan_op->provider_type_) {
            const node::ExprListNode* new_groups = nullptr;
            const node::ExprListNode* keys = nullptr;
            std::string index_name;
            auto& index_hint = scan_op->table_handler_->GetIndex();
            if (!TransformGroupExpr(schema_sources, group->keys(), index_hint,
                                    &index_name, &keys, &new_groups)) {
                return false;
            }
            PhysicalPartitionProviderNode* partition_op =
                new PhysicalPartitionProviderNode(scan_op, index_name);
            node_manager_->RegisterNode(partition_op);
            *new_in = partition_op;
            group->set_keys(new_groups);
            return true;
        }
    } else if (kPhysicalOpSimpleProject == in->type_) {
        auto simple_project = dynamic_cast<PhysicalSimpleProjectNode*>(in);
        PhysicalOpNode* new_depend;
        if (!GroupOptimized(simple_project->GetOutputNameSchemaList(),
                            simple_project->producers()[0], group,
                            &new_depend)) {
            return false;
        }
        PhysicalSimpleProjectNode* new_simple_op =
            new PhysicalSimpleProjectNode(
                new_depend, simple_project->output_schema_,
                simple_project->project_.column_sources());
        *new_in = new_simple_op;
        return true;
    }

    return false;
}

bool GroupAndSortOptimized::SortOptimized(
    const vm::SchemaSourceList& schema_sources, PhysicalOpNode* in,
    Sort* sort) {
    if (nullptr == sort) {
        return false;
    }
    if (kPhysicalOpDataProvider == in->type_) {
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
        TransformOrderExpr(schema_sources, sort->orders(),
                           *(scan_op->table_handler_->GetSchema()), index_st,
                           &new_orders);
        sort->set_orders(new_orders);
        return true;
    } else if (kPhysicalOpSimpleProject == in->type_) {
        auto simple_project = dynamic_cast<PhysicalSimpleProjectNode*>(in);
        return SortOptimized(simple_project->GetOutputNameSchemaList(),
                             simple_project->producers()[0], sort);
    }
    return false;
}

bool GroupAndSortOptimized::Transform(PhysicalOpNode* in,
                                      PhysicalOpNode** output) {
    *output = in;
    switch (in->type_) {
        case kPhysicalOpGroupBy: {
            PhysicalGroupNode* group_op = dynamic_cast<PhysicalGroupNode*>(in);
            PhysicalOpNode* new_producer;
            if (!GroupOptimized(SchemaSourceList(), group_op->GetProducer(0),
                                &group_op->group_, &new_producer)) {
                return false;
            }
            group_op->SetProducer(0, new_producer);
            if (!group_op->Valid()) {
                *output = group_op->producers()[0];
            }
            return true;
        }
        case kPhysicalOpProject: {
            auto project_op = dynamic_cast<PhysicalProjectNode*>(in);
            if (kWindowAggregation == project_op->project_type_) {
                PhysicalWindowAggrerationNode* union_op =
                    dynamic_cast<PhysicalWindowAggrerationNode*>(project_op);

                PhysicalOpNode* new_producer;

                if (GroupOptimized(SchemaSourceList(), union_op->GetProducer(0),
                                   &union_op->window_.partition_,
                                   &new_producer)) {
                    union_op->SetProducer(0, new_producer);
                }
                SortOptimized(SchemaSourceList(), union_op->GetProducer(0),
                              &union_op->window_.sort_);

                if (!union_op->window_joins().Empty()) {
                    for (auto& window_join :
                         union_op->window_joins().window_joins()) {
                        PhysicalOpNode* new_join_right;
                        if (JoinKeysOptimized(
                                SchemaSourceList(), window_join.first,
                                &window_join.second, &new_join_right)) {
                            window_join.first = new_join_right;
                        }
                        SortOptimized(SchemaSourceList(), window_join.first,
                                      &window_join.second.right_sort_);
                    }
                }
                if (!union_op->window_unions().Empty()) {
                    for (auto& window_union :
                         union_op->window_unions().window_unions_) {
                        PhysicalOpNode* new_producer;

                        if (GroupOptimized(SchemaSourceList(),
                                           window_union.first,
                                           &window_union.second.partition_,
                                           &new_producer)) {
                            window_union.first = new_producer;
                        }
                        SortOptimized(SchemaSourceList(), window_union.first,
                                      &window_union.second.sort_);
                    }
                }
                return true;
            }
            break;
        }
        case kPhysicalOpRequestUnoin: {
            PhysicalRequestUnionNode* union_op =
                dynamic_cast<PhysicalRequestUnionNode*>(in);
            PhysicalOpNode* new_producer;
            if (KeysFilterOptimized(
                    SchemaSourceList(), union_op->GetProducer(1),
                    &union_op->window_.partition_,
                    &union_op->window_.index_key_, &new_producer)) {
                union_op->SetProducer(1, new_producer);
            }
            SortOptimized(SchemaSourceList(), union_op->GetProducer(1),
                          &union_op->window_.sort_);

            if (!union_op->window_unions().Empty()) {
                for (auto& window_union :
                     union_op->window_unions_.window_unions_) {
                    PhysicalOpNode* new_producer;
                    auto& window = window_union.second;
                    if (KeysFilterOptimized(
                            SchemaSourceList(), window_union.first,
                            &window.partition_, &window.index_key_,
                            &new_producer)) {
                        window_union.first = new_producer;
                    }
                    SortOptimized(SchemaSourceList(), window_union.first,
                                  &window.sort_);
                }
            }
            return true;
        }
        case kPhysicalOpRequestJoin: {
            PhysicalRequestJoinNode* join_op =
                dynamic_cast<PhysicalRequestJoinNode*>(in);
            PhysicalOpNode* new_producer;
            // Optimized Right Table Partition
            if (!JoinKeysOptimized(SchemaSourceList(), join_op->GetProducer(1),
                                   &join_op->join_, &new_producer)) {
                return false;
            }
            join_op->SetProducer(1, new_producer);
            SortOptimized(SchemaSourceList(), join_op->GetProducer(1),
                          &join_op->join_.right_sort_);
            return true;
        }
        case kPhysicalOpJoin: {
            PhysicalJoinNode* join_op = dynamic_cast<PhysicalJoinNode*>(in);
            PhysicalOpNode* new_producer;
            // Optimized Right Table Partition
            if (!JoinKeysOptimized(SchemaSourceList(), join_op->GetProducer(1),
                                   &join_op->join_, &new_producer)) {
                return false;
            }
            join_op->SetProducer(1, new_producer);
            SortOptimized(SchemaSourceList(), join_op->GetProducer(1),
                          &join_op->join_.right_sort_);
            return true;
        }
        case kPhysicalOpFilter: {
            PhysicalFliterNode* filter_op =
                dynamic_cast<PhysicalFliterNode*>(in);
            PhysicalOpNode* new_producer;
            if (FilterOptimized(SchemaSourceList(), filter_op->GetProducer(0),
                                &filter_op->filter_, &new_producer)) {
                filter_op->SetProducer(0, new_producer);
            }
        }
        default: {
            return false;
        }
    }
    return false;
}
bool GroupAndSortOptimized::TransformGroupExpr(
    const vm::SchemaSourceList& schema_sources,
    const node::ExprListNode* groups, const IndexHint& index_hint,
    std::string* index_name, const node::ExprListNode** keys_expr,
    const node::ExprListNode** output) {
    if (nullptr == groups || nullptr == output || nullptr == index_name) {
        LOG(WARNING) << "fail to transform group expr : group exor or output "
                        "or index_name ptr is null";
        return false;
    }
    SchemasContext schema_ctx(schema_sources);
    std::vector<std::string> columns;
    for (auto group : groups->children_) {
        switch (group->expr_type_) {
            case node::kExprColumnRef: {
                auto column = dynamic_cast<node::ColumnRefNode*>(group);
                columns.push_back(schema_ctx.SourceColumnNameResolved(column));
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

    std::vector<bool> bitmap(columns.size(), true);
    if (MatchBestIndex(columns, index_hint, &bitmap, index_name)) {
        IndexSt index = index_hint.at(*index_name);
        node::ExprListNode* new_groups = node_manager_->MakeExprList();
        std::set<std::string> keys;
        for (auto iter = index.keys.cbegin(); iter != index.keys.cend();
             iter++) {
            keys.insert(iter->name);
        }

        // generate index keys expr
        auto key_expr_list = node_manager_->MakeExprList();
        for (auto iter = index.keys.cbegin(); iter != index.keys.cend();
             iter++) {
            for (auto group : groups->children_) {
                switch (group->expr_type_) {
                    case node::kExprColumnRef: {
                        auto column = dynamic_cast<node::ColumnRefNode*>(group);
                        std::string column_name =
                            schema_ctx.SourceColumnNameResolved(column);
                        // skip group when match index keys
                        if (column_name == iter->name) {
                            key_expr_list->AddChild(group);
                            break;
                        }
                        break;
                    }
                    default: {
                        new_groups->AddChild(group);
                    }
                }
            }
        }

        // generate new groups expr
        for (auto expr : groups->children_) {
            switch (expr->expr_type_) {
                case node::kExprColumnRef: {
                    std::string column = schema_ctx.SourceColumnNameResolved(
                        dynamic_cast<node::ColumnRefNode*>(expr));
                    // skip group when match index keys
                    if (keys.find(column) == keys.cend()) {
                        new_groups->AddChild(expr);
                    }
                    break;
                }
                default: {
                    new_groups->AddChild(expr);
                }
            }
        }
        *keys_expr = key_expr_list;
        *output = new_groups;
        return true;
    } else {
        return false;
    }
}

bool GroupAndSortOptimized::MatchBestIndex(
    const std::vector<std::string>& columns, const IndexHint& index_hint,
    std::vector<bool>* bitmap_ptr, std::string* index_name) {
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
            return true;
        }
    }

    std::string best_index_name;
    bool succ = false;
    for (size_t i = 0; i < bitmap.size(); ++i) {
        if (bitmap[i]) {
            bitmap[i] = false;
            std::string name;
            if (MatchBestIndex(columns, index_hint, bitmap_ptr, &name)) {
                succ = true;
                if (best_index_name.empty()) {
                    best_index_name = name;
                } else {
                    auto org_index = index_hint.at(best_index_name);
                    auto new_index = index_hint.at(name);
                    if (org_index.keys.size() < new_index.keys.size()) {
                        best_index_name = name;
                    }
                }
            }
            bitmap[i] = true;
        }
    }
    *index_name = best_index_name;
    return succ;
}

bool ConditionOptimized::JoinConditionOptimized(PhysicalOpNode* in,
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
    if (!TransformEqualExprPair(in->GetOutputNameSchemaList(),
                                in->producers()[0]->GetOutputSchemaListSize(),
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
    if (!TransformConstEqualExprPair(in->GetOutputNameSchemaList(),
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
    filter->left_key_.set_keys(left_keys);
    filter->right_key_.set_keys(right_keys);
    filter->condition_.set_condition(filter_condition);
    return true;
}
bool ConditionOptimized::Transform(PhysicalOpNode* in,
                                   PhysicalOpNode** output) {
    *output = in;
    switch (in->type_) {
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
            PhysicalFliterNode* filter_op =
                dynamic_cast<PhysicalFliterNode*>(in);
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
    const vm::SchemasContext& ctx, const size_t min_schema_id,
    ExprPair* output) {
    bool is_first_const = node::ExprIsConst(expr_pair.first);
    bool is_second_const = node::ExprIsConst(expr_pair.second);

    if (is_first_const && is_second_const) {
        return false;
    } else if (is_first_const) {
        // resolved second expr
        const RowSchemaInfo* info_second;
        if (!ctx.ExprRefResolved(expr_pair.second, &info_second)) {
            return false;
        }
        if (min_schema_id <= info_second->idx_) {
            // const expr = right_table_expr
            *output = {expr_pair.first, expr_pair.second};
            return true;
        } else {
            return false;
        }
    } else if (is_second_const) {
        // resolved first expr
        const RowSchemaInfo* info_first;
        if (!ctx.ExprRefResolved(expr_pair.first, &info_first)) {
            return false;
        }
        if (min_schema_id <= info_first->idx_) {
            // right_table_expr = const expr
            *output = {expr_pair.second, expr_pair.first};
            return true;
        } else {
            return false;
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
    const SchemaSourceList& name_schema_list,
    node::ExprListNode* and_conditions, node::ExprListNode* out_condition_list,
    std::vector<ExprPair>& condition_eq_pair) {  // NOLINT
    vm::SchemasContext ctx(name_schema_list);
    for (auto expr : and_conditions->children_) {
        std::pair<node::ExprNode*, node::ExprNode*> expr_pair;
        if (!ExtractEqualExprPair(expr, &expr_pair)) {
            out_condition_list->AddChild(expr);
            continue;
        }
        ExprPair const_pair;
        if (MakeConstEqualExprPair(expr_pair, ctx, 0, &const_pair)) {
            condition_eq_pair.push_back(const_pair);
        } else {
            out_condition_list->AddChild(expr);
        }
    }
    return !condition_eq_pair.empty();
}
// Return Equal Expression Pair
// Left Expr should belongs to first schema
bool ConditionOptimized::TransformEqualExprPair(
    const SchemaSourceList& name_schema_list, const size_t left_schema_cnt,
    node::ExprListNode* and_conditions, node::ExprListNode* out_condition_list,
    std::vector<ExprPair>& condition_eq_pair) {  // NOLINT
    vm::SchemasContext ctx(name_schema_list);
    for (auto expr : and_conditions->children_) {
        std::pair<node::ExprNode*, node::ExprNode*> expr_pair;
        if (!ExtractEqualExprPair(expr, &expr_pair)) {
            out_condition_list->AddChild(expr);
            continue;
        }
        ExprPair const_pair;
        if (MakeConstEqualExprPair(expr_pair, ctx, left_schema_cnt,
                                   &const_pair)) {
            condition_eq_pair.push_back(const_pair);
        } else {
            const RowSchemaInfo* info_left;
            const RowSchemaInfo* info_right;
            if (!ctx.ExprRefResolved(expr_pair.first, &info_left)) {
                out_condition_list->AddChild(expr);
                continue;
            }
            if (!ctx.ExprRefResolved(expr_pair.second, &info_right)) {
                out_condition_list->AddChild(expr);
                continue;
            }
            if (nullptr == info_left || nullptr == info_right) {
                out_condition_list->AddChild(expr);
                continue;
            }
            if (info_left == info_right) {
                out_condition_list->AddChild(expr);
                continue;
            }
            if (left_schema_cnt > info_left->idx_) {
                ExprPair pair = {expr_pair.first, expr_pair.second};
                condition_eq_pair.push_back(pair);
            } else if (left_schema_cnt > info_right->idx_) {
                ExprPair pair = {expr_pair.second, expr_pair.first};
                condition_eq_pair.push_back(pair);
            } else {
                out_condition_list->AddChild(expr);
                continue;
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
    if (kPhysicalOpLimit != in->type_) {
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
    if (kPhysicalOpLimit == node->type_) {
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
    if (node->type_ == kPhysicalOpSimpleProject) {
        return false;
    }
    if (node->is_block_) {
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
// This is primarily intended to be used on top-level WHERE (or JOIN/ON)
// clauses.  It can also be used on top-level CHECK constraints, for which
// pass is_check = true.  DO NOT call it on any expression that is not known
// to be one or the other, as it might apply inappropriate simplifications.
bool CanonicalizeExprTransformPass::Transform(node::ExprNode* in,
                                              node::ExprNode** output) {
    // 1. 忽略NULL以及OR中的False/AND中的TRUE
    // 2. 拉平谓词
    // 3. 清除重复ORs
    // 4.
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
            in->SetProducer(j, output);
        }
    }
    return Transform(in, out);
}

bool GroupAndSortOptimized::TransformOrderExpr(
    const vm::SchemaSourceList& schema_sources, const node::OrderByNode* order,
    const Schema& schema, const IndexSt& index_st,
    const node::OrderByNode** output) {
    if (nullptr == order || nullptr == output) {
        LOG(WARNING) << "fail to transform order expr : order expr or "
                        "data_provider op "
                        "or output is null";
        return false;
    }

    auto& ts_column = schema.Get(index_st.ts_pos);
    std::vector<std::string> columns;
    *output = order;
    bool succ = false;
    SchemasContext schema_ctx(schema_sources);
    for (auto expr : order->order_by()->children_) {
        switch (expr->expr_type_) {
            case node::kExprColumnRef: {
                std::string column_name = schema_ctx.SourceColumnNameResolved(
                    dynamic_cast<node::ColumnRefNode*>(expr));
                columns.push_back(column_name);
                if (ts_column.name() == column_name) {
                    succ = true;
                }
                break;
            }
            default: {
                break;
            }
        }
    }

    if (succ) {
        node::ExprListNode* expr_list = node_manager_->MakeExprList();
        for (auto expr : order->order_by_->children_) {
            switch (expr->expr_type_) {
                case node::kExprColumnRef: {
                    std::string column_name =
                        schema_ctx.SourceColumnNameResolved(
                            dynamic_cast<node::ColumnRefNode*>(expr));
                    // skip group when match index keys
                    if (ts_column.name() != column_name) {
                        expr_list->AddChild(expr);
                    }
                    break;
                }
                default: {
                    expr_list->AddChild(expr);
                }
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
        kPhysicalOpJoin != in->producers()[0]->type_) {
        return false;
    }
    PhysicalJoinNode* join_op =
        dynamic_cast<PhysicalJoinNode*>(in->producers()[0]);

    auto join_type = join_op->join().join_type();
    if (node::kJoinTypeLeft != join_type && node::kJoinTypeLast != join_type) {
        // skip optimized for other join type
        return false;
    }
    switch (in->type_) {
        case kPhysicalOpGroupBy: {
            auto group_op = dynamic_cast<PhysicalGroupNode*>(in);
            if (node::ExprListNullOrEmpty(group_op->group_.keys_)) {
                LOG(WARNING) << "Join optimized skip: groups is null or empty";
            }

            if (!CheckExprListFromSchema(
                    group_op->group_.keys_,
                    (join_op->GetProducers()[0]->output_schema_))) {
                return false;
            }
            auto group_expr = group_op->group_.keys_;
            // 符合优化条件
            PhysicalGroupNode* new_group_op =
                new PhysicalGroupNode(join_op->producers()[0], group_expr);
            PhysicalJoinNode* new_join_op = new PhysicalJoinNode(
                new_group_op, join_op->GetProducers()[1], join_op->join_);
            node_manager_->RegisterNode(new_group_op);
            node_manager_->RegisterNode(new_join_op);
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
                    join_op->GetProducers()[0]->output_schema_)) {
                return false;
            }
            // 符合优化条件
            PhysicalSortNode* new_order_op =
                new PhysicalSortNode(join_op->producers()[0], sort_op->sort_);
            node_manager_->RegisterNode(new_order_op);
            PhysicalJoinNode* new_join_op = new PhysicalJoinNode(
                new_order_op, join_op->GetProducers()[1], join_op->join_);
            node_manager_->RegisterNode(new_order_op);
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
            SchemasContext ctx(
                join_op->producers()[0]->GetOutputNameSchemaList());

            const RowSchemaInfo* info;
            if (!ctx.ExprListResolvedFromSchema(
                    window_agg_op->window_.partition_.keys_->children_,
                    &info) ||
                info->idx_ != 0) {
                LOG(WARNING) << "Window Join optimized skip: partition keys "
                                "are resolved from secondary table";
                return false;
            }

            if (!ctx.ExprListResolvedFromSchema(
                    window_agg_op->window_.sort_.orders_->order_by_->children_,
                    &info) ||
                info->idx_ != 0) {
                LOG(WARNING) << "Window Join optimized skip: order keys are "
                                "resolved from secondary table";
                return false;
            }

            auto left = join_op->producers()[0];
            auto right = join_op->producers()[1];
            window_agg_op->SetProducer(0, left);
            window_agg_op->AddWindowJoin(right, join_op->join());
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
    const node::ExprListNode* expr_list, const Schema& schema) {
    if (node::ExprListNullOrEmpty(expr_list)) {
        return true;
    }

    for (auto expr : expr_list->children_) {
        switch (expr->expr_type_) {
            case node::kExprColumnRef: {
                auto column = dynamic_cast<node::ColumnRefNode*>(expr);
                if (!ColumnExist(schema, column->GetColumnName())) {
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

RequestModeransformer::RequestModeransformer(
    node::NodeManager* node_manager, const std::string& db,
    const std::shared_ptr<Catalog>& catalog, ::llvm::Module* module,
    udf::UDFLibrary* library, const bool performance_sensitive)
    : BatchModeTransformer(node_manager, db, catalog, module, library,
                           performance_sensitive) {}

RequestModeransformer::~RequestModeransformer() {}

// transform project plan in request mode
bool RequestModeransformer::TransformProjecPlantOp(
    const node::ProjectPlanNode* node, PhysicalOpNode** output,
    base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }
    PhysicalOpNode* depend = nullptr;
    if (!TransformPlanOp(node->GetChildren()[0], &depend, status)) {
        return false;
    }

    std::vector<PhysicalOpNode*> ops;
    for (auto iter = node->project_list_vec_.cbegin();
         iter != node->project_list_vec_.cend(); iter++) {
        fesql::node::ProjectListNode* project_list =
            dynamic_cast<fesql::node::ProjectListNode*>(*iter);

        PhysicalOpNode* project_op = nullptr;
        if (!TransformProjectOp(project_list, depend, &project_op, status)) {
            return false;
        }
        ops.push_back(project_op);
    }

    if (ops.empty()) {
        status.msg = "fail transform project op: empty projects";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }

    if (ops.size() == 1) {
        *output = ops[0];
        return true;
    } else {
        auto iter = ops.cbegin();

        PhysicalOpNode* join = new PhysicalRequestJoinNode(
            (*iter), *(++iter), ::fesql::node::kJoinTypeConcat);
        node_manager_->RegisterNode(join);
        iter++;
        for (; iter != ops.cend(); iter++) {
            join = new PhysicalRequestJoinNode(join, *iter,
                                               ::fesql::node::kJoinTypeConcat);
            node_manager_->RegisterNode(join);
        }

        auto project_list =
            node_manager_->MakeProjectListPlanNode(nullptr, false);
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
                    for (auto column : depend->output_schema_) {
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
                    node_manager_->MakeColumnRefNode(project_node->GetName(),
                                                     "")));
            }
            pos++;
        }

        return CreatePhysicalProjectNode(kRowProject, join, project_list,
                                         output, status);
    }
}

bool RequestModeransformer::TransformJoinOp(const node::JoinPlanNode* node,
                                            PhysicalOpNode** output,
                                            base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }
    PhysicalOpNode* left = nullptr;
    PhysicalOpNode* right = nullptr;
    if (!TransformPlanOp(node->GetChildren()[0], &left, status)) {
        return false;
    }
    if (!TransformPlanOp(node->GetChildren()[1], &right, status)) {
        return false;
    }
    *output = new PhysicalRequestJoinNode(left, right, node->join_type_,
                                          node->orders_, node->condition_);

    node_manager_->RegisterNode(*output);
    auto check_status = CheckTimeOrIntegerOrderColumn(
        node->orders_, (*output)->GetOutputNameSchemaList());
    if (!check_status.isOK()) {
        status = check_status;
        return false;
    }
    return true;
}
bool RequestModeransformer::TransformScanOp(const node::TablePlanNode* node,
                                            PhysicalOpNode** output,
                                            base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }
    if (node->IsPrimary()) {
        auto table = catalog_->GetTable(db_, node->table_);
        if (table) {
            *output = new PhysicalRequestProviderNode(table);
            node_manager_->RegisterNode(*output);
            request_schema_ = *table->GetSchema();
            request_name_ = table->GetName();
            return true;
        } else {
            status.msg = "fail to transform data_provider op: table " + db_ +
                         "." + node->table_ + " not exist!";
            status.code = common::kPlanError;
            LOG(WARNING) << status;
            return false;
        }
    }
    return BatchModeTransformer::TransformScanOp(node, output, status);
}
bool RequestModeransformer::TransformProjectOp(
    node::ProjectListNode* project_list, PhysicalOpNode* depend,
    PhysicalOpNode** output, base::Status& status) {
    if (nullptr != project_list->w_ptr_) {
        if (!TransformWindowOp(depend, project_list->w_ptr_, &depend, status)) {
            return false;
        }
    }
    switch (depend->output_type_) {
        case kSchemaTypeRow:
            return CreatePhysicalProjectNode(kRowProject, depend, project_list,
                                             output, status);
        case kSchemaTypeGroup:
            return CreatePhysicalProjectNode(kGroupAggregation, depend,
                                             project_list, output, status);
        case kSchemaTypeTable:
            if (project_list->is_window_agg_) {
                return CreatePhysicalProjectNode(kAggregation, depend,
                                                 project_list, output, status);
            } else {
                return CreatePhysicalProjectNode(kTableProject, depend,
                                                 project_list, output, status);
            }
    }
    return false;
}

bool SimpleProjectOptimized::Transform(PhysicalOpNode* in,
                                       PhysicalOpNode** output) {
    *output = in;
    switch (in->type_) {
        case kPhysicalOpSimpleProject: {
            PhysicalSimpleProjectNode* simple_project =
                dynamic_cast<PhysicalSimpleProjectNode*>(in);
            if (nullptr != in->producers()[0] &&
                kPhysicalOpSimpleProject == in->producers()[0]->type_) {
                auto depend = dynamic_cast<PhysicalSimpleProjectNode*>(
                    in->producers()[0]);
                ColumnSourceList new_sources;
                if (false == ColumnProject::CombineColumnSources(
                                 simple_project->project().column_sources(),
                                 depend->project().column_sources(),
                                 new_sources)) {
                    LOG(WARNING) << "Fail to Combine Simple Project";
                    return false;
                }
                *output = new PhysicalSimpleProjectNode(
                    depend->producers()[0], in->output_schema_, new_sources);
                node_manager_->RegisterNode(*output);
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
