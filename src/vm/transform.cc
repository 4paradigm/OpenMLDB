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
#include "codegen/fn_let_ir_builder.h"
#include "vm/physical_op.h"

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
        LOG(WARNING) << status.msg;
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

Transform::Transform(node::NodeManager* node_manager, const std::string& db,
                     const std::shared_ptr<Catalog>& catalog,
                     ::llvm::Module* module)
    : node_manager_(node_manager),
      db_(db),
      catalog_(catalog),
      module_(module),
      id_(0) {}

Transform::~Transform() {}

bool Transform::TransformPlanOp(const ::fesql::node::PlanNode* node,
                                ::fesql::vm::PhysicalOpNode** ouput,
                                ::fesql::base::Status& status) {
    if (nullptr == node || nullptr == ouput) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status.msg;
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
            LOG(WARNING) << status.msg;
            return false;
        }
    }
    if (!ok) {
        return false;
    }
    op_map_[logical_op] = op;
    *ouput = op;
    return true;
}

bool Transform::TransformLimitOp(const node::LimitPlanNode* node,
                                 PhysicalOpNode** output,
                                 base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status.msg;
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

bool Transform::TransformProjecPlantOp(const node::ProjectPlanNode* node,
                                       PhysicalOpNode** output,
                                       base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status.msg;
        return false;
    }
    PhysicalOpNode* depend = nullptr;
    if (!TransformPlanOp(node->GetChildren()[0], &depend, status)) {
        return false;
    }

    if (node->project_list_vec_.empty()) {
        status.msg = "fail transform project op: empty projects";
        status.code = common::kPlanError;
        LOG(WARNING) << status.msg;
        return false;
    }

    if (1 == node->project_list_vec_.size()) {
        return TransformProjectOp(dynamic_cast<fesql::node::ProjectListNode*>(
                                      node->project_list_vec_[0]),
                                  depend, output, status);
    }

    std::vector<PhysicalOpNode*> ops;
    for (auto iter = node->project_list_vec_.rbegin();
         iter != node->project_list_vec_.rend(); ++iter) {
        fesql::node::ProjectListNode* project_list =
            dynamic_cast<fesql::node::ProjectListNode*>(*iter);

        // append oringinal table column after project columns
        // if there is multi
        if (node->project_list_vec_.size() > 1) {
            project_list->AddProject(node_manager_->MakeRowProjectNode(
                project_list->GetProjects().size(), "*",
                node_manager_->MakeAllNode("")));
        }

        PhysicalOpNode* project_op;
        if (!TransformProjectOp(project_list, depend, &project_op, status)) {
            return false;
        }
        depend = project_op;
    }

    auto project_list = node_manager_->MakeProjectListPlanNode(nullptr);
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
                for (auto column : depend->output_schema) {
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

    return CreatePhysicalProjectNode(kTableProject, depend, project_list,
                                     output, status);
}
bool Transform::TransformGroupAndSortOp(
    const node::ProjectListNode* project_list, PhysicalOpNode* depend,
    PhysicalOpNode** output, base::Status& status) {
    if (nullptr == project_list || nullptr == project_list->w_ptr_ ||
        nullptr == output) {
        status.msg = "project node or window node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status.msg;
        return false;
    }

    node::OrderByNode* orders = nullptr;
    node::ExprListNode* groups = nullptr;
    if (!project_list->w_ptr_->GetOrders().empty()) {
        // TODO(chenjing): remove 临时适配, 有mem泄漏问题
        node::ExprListNode* expr = new node::ExprListNode();
        for (auto id : project_list->w_ptr_->GetOrders()) {
            expr->AddChild(new node::ColumnRefNode(id, ""));
        }
        orders = dynamic_cast<node::OrderByNode*>(
            node_manager_->MakeOrderByNode(expr, true));
    }

    if (!project_list->w_ptr_->GetKeys().empty()) {
        // TODO(chenjing): remove 临时适配, 有mem泄漏问题
        groups = node_manager_->MakeExprList();
        for (auto id : project_list->w_ptr_->GetKeys()) {
            groups->AddChild(node_manager_->MakeColumnRefNode(id, ""));
        }
    }

    if (nullptr == orders && nullptr == groups) {
        status.msg =
            "fail to transform window aggeration: gourps and orders is null";
        LOG(WARNING) << status.msg;
        return false;
    }

    PhysicalGroupAndSortNode* group_sort_op =
        new PhysicalGroupAndSortNode(depend, groups, orders);
    node_manager_->RegisterNode(group_sort_op);
    *output = group_sort_op;
    return true;
}
bool Transform::TransformJoinOp(const node::JoinPlanNode* node,
                                PhysicalOpNode** output, base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status.msg;
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
    *output =
        new PhysicalJoinNode(left, right, node->join_type_, node->condition_);
    node_manager_->RegisterNode(*output);
    return true;
}
bool Transform::TransformUnionOp(const node::UnionPlanNode* node,
                                 PhysicalOpNode** output,
                                 base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status.msg;
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
bool Transform::TransformGroupOp(const node::GroupPlanNode* node,
                                 PhysicalOpNode** output,
                                 base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status.msg;
        return false;
    }
    PhysicalOpNode* left = nullptr;
    if (!TransformPlanOp(node->GetChildren()[0], &left, status)) {
        return false;
    }
    *output = new PhysicalGroupNode(left, node->by_list_);
    node_manager_->RegisterNode(*output);
    return true;
}
bool Transform::TransformSortOp(const node::SortPlanNode* node,
                                PhysicalOpNode** output, base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status.msg;
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
bool Transform::TransformFilterOp(const node::FilterPlanNode* node,
                                  PhysicalOpNode** output,
                                  base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status.msg;
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

bool Transform::TransformScanOp(const node::TablePlanNode* node,
                                PhysicalOpNode** output, base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status.msg;
        return false;
    }
    auto table = catalog_->GetTable(db_, node->table_);
    if (table) {
        if (node->IsPrimary()) {
            *output = new PhysicalRequestProviderNode(table);
        } else {
            *output = new PhysicalTableProviderNode(table);
        }
        node_manager_->RegisterNode(*output);
        return true;
    } else {
        status.msg = "fail to transform scan op: table " + db_ + "." +
                     node->table_ + " not exist!";
        status.code = common::kPlanError;
        LOG(WARNING) << status.msg;
        return false;
    }
}

bool Transform::TransformRenameOp(const node::RenamePlanNode* node,
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
    *output = new PhysicalRenameNode(left, node->table_);
    node_manager_->RegisterNode(*output);
    return true;
}
bool Transform::TransformQueryPlan(const node::QueryPlanNode* node,
                                   PhysicalOpNode** output,
                                   base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        return false;
    }
    return TransformPlanOp(node->GetChildren()[0], output, status);
}
bool Transform::TransformDistinctOp(const node::DistinctPlanNode* node,
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
    *output = new PhysicalDistinctNode(left);
    node_manager_->RegisterNode(*output);
    return true;
}
bool Transform::TransformBatchPhysicalPlan(::fesql::node::PlanNode* node,
                                           ::fesql::vm::PhysicalOpNode** output,
                                           ::fesql::base::Status& status) {
    PhysicalOpNode* physical_plan;
    if (!TransformPlanOp(node, &physical_plan, status)) {
        return false;
    }

    for (auto type : passes) {
        switch (type) {
            case kPassGroupAndSortOptimized: {
                GroupAndSortOptimized pass(node_manager_, db_, catalog_);
                PhysicalOpNode* new_op;
                if (pass.Apply(physical_plan, &new_op)) {
                    physical_plan = new_op;
                }
                break;
            }
            case kPassLeftJoinOptimized: {
                LeftJoinOptimized pass(node_manager_, db_, catalog_);
                PhysicalOpNode* new_op;
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
    return true;
}
bool Transform::AddPass(PhysicalPlanPassType type) {
    passes.push_back(type);
    return true;
}
bool Transform::GenProjects(const Schema& input_schema,
                            const node::PlanNodeList& projects,
                            const bool row_project,
                            std::string& fn_name,    // NOLINT
                            Schema& output_schema,   // NOLINT
                            base::Status& status) {  // NOLINT
    // TODO(wangtaize) use ops end op output schema
    ::fesql::codegen::RowFnLetIRBuilder builder(input_schema, module_);
    fn_name = "__internal_sql_codegen_" + std::to_string(id_++);
    bool ok = builder.Build(fn_name, projects, row_project, output_schema);
    if (!ok) {
        status.code = common::kCodegenError;
        status.msg = "fail to codegen projects node";
        LOG(WARNING) << status.msg;
        return false;
    }
    return true;
}
bool Transform::AddDefaultPasses() {
    AddPass(kPassLeftJoinOptimized);
    AddPass(kPassGroupAndSortOptimized);
    return false;
}

bool Transform::CreatePhysicalProjectNode(const ProjectType project_type,
                                          PhysicalOpNode* node,
                                          node::ProjectListNode* project_list,
                                          PhysicalOpNode** output,
                                          base::Status& status) {
    if (nullptr == project_list || nullptr == output) {
        status.msg = "project node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status.msg;
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
                for (auto column : node->output_schema) {
                    all_expr->children_.push_back(
                        node_manager_->MakeColumnRefNode(
                            column.name(), all_expr->GetRelationName()));
                }
            }
            has_all_project = true;
        }
    }

    if (has_all_project && 1 == projects.size()) {
        // skip project
        DLOG(INFO) << "skip project node: project has only kAllExpr "
                      "expression";
        *output = node;
        return true;
    }

    Schema output_schema;
    std::string fn_name;
    switch (project_type) {
        case kRowProject:
        case kTableProject: {
            if (!GenProjects(node->output_schema, projects, true, fn_name,
                             output_schema, status)) {
                return false;
            }
            break;
        }
        case kAggregation:
        case kGroupAggregation: {
            if (!GenProjects(node->output_schema, projects, false, fn_name,
                             output_schema, status)) {
                return false;
            }
            break;
        }
        case kWindowAggregation: {
            // TODO(chenjing): gen window aggregation
            if (!GenProjects(node->output_schema, projects, false, fn_name,
                             output_schema, status)) {
                return false;
            }
            break;
        }
    }

    PhysicalOpNode* op = nullptr;
    switch (project_type) {
        case kRowProject: {
            op = new PhysicalRowProjectNode(node, fn_name, output_schema);
            break;
        }
        case kTableProject: {
            op = new PhysicalTableProjectNode(node, fn_name, output_schema);
            break;
        }
        case kAggregation: {
            op = new PhysicalAggrerationNode(node, fn_name, output_schema);
            break;
        }
        case kGroupAggregation: {
            auto group_op = dynamic_cast<PhysicalGroupNode*>(node);
            op = new PhysicalGroupAggrerationNode(node, group_op->groups_,
                                                  fn_name, output_schema);
            break;
        }
        case kWindowAggregation: {
            PhysicalOpNode* depend = nullptr;
            if (!TransformGroupAndSortOp(project_list, node, &depend, status)) {
                return false;
            }
            op = new PhysicalWindowAggrerationNode(
                depend,
                dynamic_cast<PhysicalGroupAndSortNode*>(depend)->groups_,
                dynamic_cast<PhysicalGroupAndSortNode*>(depend)->orders_,
                fn_name, output_schema, project_list->w_ptr_->GetStartOffset(),
                project_list->w_ptr_->GetEndOffset());
            break;
        }
    }
    node_manager_->RegisterNode(op);
    *output = op;
    return true;
}

bool Transform::TransformRequestPhysicalPlan(
    ::fesql::node::PlanNode* node, ::fesql::vm::PhysicalOpNode** output,
    ::fesql::base::Status& status) {
    // return false if Primary path check fail
    ::fesql::node::PlanNode* primary_node;
    if (!ValidatePriimaryPath(node, &primary_node, status)) {
        return false;
    }

    PhysicalOpNode* physical_plan;
    if (!TransformPlanOp(node, &physical_plan, status)) {
        return false;
    }

    *output = physical_plan;
    return true;
}
bool Transform::ValidatePriimaryPath(node::PlanNode* node,
                                     node::PlanNode** output,
                                     base::Status& status) {
    if (nullptr == node) {
        status.msg = "primary path validate fail: node or output is null";
        status.code = common::kPlanError;
        return false;
    }

    switch (node->type_) {
        case node::kPlanTypeJoin:
        case node::kPlanTypeUnion: {
            auto binary_op = dynamic_cast<node::BinaryPlanNode*>(node);
            node::PlanNode* left_primary_table = nullptr;
            if (!ValidatePriimaryPath(binary_op->GetLeft(), &left_primary_table,
                                      status)) {
                return false;
            }

            node::PlanNode* right_primary_table = nullptr;
            if (!ValidatePriimaryPath(binary_op->GetRight(),
                                      &right_primary_table, status)) {
                status.msg =
                    "primary path validate fail: right path isn't valid";
                status.code = common::kPlanError;
                return false;
            }

            if (left_primary_table == right_primary_table) {
                *output = left_primary_table;
                return true;
            } else {
                status.msg =
                    "primary path validate fial: left path and right path has "
                    "different source";
                status.code = common::kPlanError;
                return false;
            }
        }
        case node::kPlanTypeTable: {
            auto table_op = dynamic_cast<node::TablePlanNode*>(node);
            table_op->SetIsPrimary(true);
            return true;
        }
        case node::kPlanTypeCreate:
        case node::kPlanTypeInsert:
        case node::kPlanTypeCmd:
        case node::kPlanTypeWindow:
        case node::kProjectList:
        case node::kProjectNode: {
            status.msg =
                "primary path validate fail: invalid node of primary path";
            status.code = common::kPlanError;
            return false;
        }
        default: {
            auto unary_op = dynamic_cast<const node::UnaryPlanNode*>(node);
            return ValidatePriimaryPath(unary_op->GetDepend(), output, status);
        }
    }
    return false;
}
bool Transform::TransformProjectOp(node::ProjectListNode* project_list,
                                   PhysicalOpNode* depend,
                                   PhysicalOpNode** output,
                                   base::Status& status) {
    if (project_list->is_window_agg_ && nullptr != project_list->w_ptr_) {
        return CreatePhysicalProjectNode(kWindowAggregation, depend,
                                         project_list, output, status);
    } else if (kPhysicalOpGroupBy == depend->type_) {
        // TODO(chenjing): 基于聚合函数分组
        return CreatePhysicalProjectNode(kGroupAggregation, depend,
                                         project_list, output, status);
    } else {
        return CreatePhysicalProjectNode(kTableProject, depend, project_list,
                                         output, status);
    }
}

bool GroupAndSortOptimized::Transform(PhysicalOpNode* in,
                                      PhysicalOpNode** output) {
    switch (in->type_) {
        case kPhysicalOpGroupBy: {
            PhysicalGroupNode* group_op = dynamic_cast<PhysicalGroupNode*>(in);
            if (kPhysicalOpDataProvider == in->GetProducers()[0]->type_) {
                auto scan_op = dynamic_cast<PhysicalDataProviderNode*>(
                    in->GetProducers()[0]);
                if (kProviderTypeTable == scan_op->provider_type_) {
                    std::string index_name;
                    const node::ExprListNode* new_groups;
                    if (!TransformGroupExpr(group_op->groups_,
                                            scan_op->table_handler_->GetIndex(),
                                            &index_name, &new_groups)) {
                        return false;
                    }

                    PhysicalScanIndexNode* scan_index_op =
                        new PhysicalScanIndexNode(scan_op->table_handler_,
                                                  index_name);
                    node_manager_->RegisterNode(scan_index_op);

                    if (nullptr == new_groups || new_groups->IsEmpty()) {
                        *output = scan_index_op;
                    } else {
                        PhysicalGroupNode* new_group_op =
                            new PhysicalGroupNode(scan_index_op, new_groups);
                        *output = new_group_op;
                    }
                    return true;
                }
            }
            break;
        }
        case kPhysicalOpGroupAndSort: {
            PhysicalGroupAndSortNode* group_sort_op =
                dynamic_cast<PhysicalGroupAndSortNode*>(in);
            if (kPhysicalOpDataProvider == in->GetProducers()[0]->type_) {
                auto scan_op = dynamic_cast<PhysicalDataProviderNode*>(
                    in->GetProducers()[0]);
                if (kProviderTypeTable == scan_op->provider_type_) {
                    std::string index_name;
                    const node::ExprListNode* new_groups;
                    const node::OrderByNode* new_orders;
                    auto& index_hint = scan_op->table_handler_->GetIndex();
                    if (!TransformGroupExpr(group_sort_op->groups_, index_hint,
                                            &index_name, &new_groups)) {
                        return false;
                    }

                    auto index_st = index_hint.at(index_name);
                    TransformOrderExpr(group_sort_op->orders_,
                                       scan_op->table_handler_->GetSchema(),
                                       index_st, &new_orders);

                    PhysicalScanIndexNode* scan_index_op =
                        new PhysicalScanIndexNode(scan_op->table_handler_,
                                                  index_name);
                    node_manager_->RegisterNode(scan_index_op);

                    if ((nullptr == new_groups || new_groups->IsEmpty()) &&
                        (nullptr == new_orders ||
                         nullptr == new_orders->order_by_ ||
                         new_orders->order_by_->IsEmpty())) {
                        *output = scan_index_op;
                    } else {
                        PhysicalGroupAndSortNode* new_group_sort_op =
                            new PhysicalGroupAndSortNode(
                                scan_index_op, new_groups, new_orders);
                        *output = new_group_sort_op;
                    }
                    return true;
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
bool GroupAndSortOptimized::TransformGroupExpr(
    const node::ExprListNode* groups, const IndexHint& index_hint,
    std::string* index_name, const node::ExprListNode** output) {
    if (nullptr == groups || nullptr == output || nullptr == index_name) {
        LOG(WARNING) << "fail to transform group expr : group exor or output "
                        "or index_name ptr is null";
        return false;
    }
    std::vector<std::string> columns;
    for (auto group : groups->children_) {
        switch (group->expr_type_) {
            case node::kExprColumnRef:
                columns.push_back(
                    dynamic_cast<node::ColumnRefNode*>(group)->GetColumnName());
                break;
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
        for (auto group : groups->children_) {
            switch (group->expr_type_) {
                case node::kExprColumnRef: {
                    std::string column =
                        dynamic_cast<node::ColumnRefNode*>(group)
                            ->GetColumnName();
                    // skip group when match index keys
                    if (keys.find(column) == keys.cend()) {
                        new_groups->AddChild(group);
                    }
                    break;
                }
                default: {
                    new_groups->AddChild(group);
                }
            }
        }
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
    auto producer = in->GetProducers();
    for (size_t j = 0; j < producer.size(); ++j) {
        PhysicalOpNode* output;
        if (Apply(producer[j], &output)) {
            in->UpdateProducer(j, output);
        }
    }
    return Transform(in, out);
}

bool GroupAndSortOptimized::TransformOrderExpr(
    const node::OrderByNode* order, const Schema& schema,
    const IndexSt& index_st, const node::OrderByNode** output) {
    if (nullptr == order || nullptr == output) {
        LOG(WARNING) << "fail to transform order expr : order expr or scan op "
                        "or output is null";
        return false;
    }

    auto& ts_column = schema.Get(index_st.ts_pos);
    std::vector<std::string> columns;
    *output = order;

    bool succ = false;
    for (auto expr : order->order_by_->children_) {
        switch (expr->expr_type_) {
            case node::kExprColumnRef:
                columns.push_back(
                    dynamic_cast<node::ColumnRefNode*>(expr)->GetColumnName());
                if (ts_column.name() ==
                    dynamic_cast<node::ColumnRefNode*>(expr)->GetColumnName()) {
                    succ = true;
                }
                break;
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
                    std::string column =
                        dynamic_cast<node::ColumnRefNode*>(expr)
                            ->GetColumnName();
                    // skip group when match index keys
                    if (ts_column.name() != column) {
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
    switch (in->type_) {
        case kPhysicalOpGroupBy: {
            auto group_op = dynamic_cast<PhysicalGroupNode*>(in);
            if (node::ExprListNullOrEmpty(group_op->groups_)) {
                LOG(WARNING)
                    << "LeftJoin optimized skip: groups is null or empty";
            }
            if (kPhysicalOpJoin == in->GetProducers()[0]->type_) {
                PhysicalJoinNode* join_op =
                    dynamic_cast<PhysicalJoinNode*>(in->GetProducers()[0]);

                if (node::kJoinTypeLeft != join_op->join_type_) {
                    // skip optimized for other join type
                    return false;
                }
                if (!CheckExprListFromSchema(
                        group_op->groups_,
                        join_op->GetProducers()[0]->output_schema)) {
                    return false;
                }
                auto group_expr = group_op->groups_;
                // 符合优化条件
                PhysicalGroupNode* new_group_op = new PhysicalGroupNode(
                    join_op->GetProducers()[0], group_expr);
                PhysicalJoinNode* new_join_op = new PhysicalJoinNode(
                    new_group_op, join_op->GetProducers()[1],
                    join_op->join_type_, join_op->condition_);
                node_manager_->RegisterNode(new_group_op);
                node_manager_->RegisterNode(new_join_op);
                *output = new_join_op;
                return true;
            }

            break;
        }
        case kPhysicalOpSortBy: {
            auto sort_op = dynamic_cast<PhysicalSortNode*>(in);
            if (nullptr == sort_op->order_ ||
                node::ExprListNullOrEmpty(sort_op->order_->order_by_)) {
                LOG(WARNING)
                    << "LeftJoin optimized skip: order is null or empty";
            }
            if (kPhysicalOpJoin == in->GetProducers()[0]->type_) {
                if (kPhysicalOpJoin == in->GetProducers()[0]->type_) {
                    PhysicalJoinNode* join_op =
                        dynamic_cast<PhysicalJoinNode*>(in->GetProducers()[0]);

                    if (node::kJoinTypeLeft != join_op->join_type_) {
                        // skip optimized for other join type
                        return false;
                    }

                    if (!CheckExprListFromSchema(
                            sort_op->order_->order_by_,
                            join_op->GetProducers()[0]->output_schema)) {
                        return false;
                    }

                    auto order_expr = sort_op->order_;
                    // 符合优化条件
                    PhysicalSortNode* new_order_op = new PhysicalSortNode(
                        join_op->GetProducers()[0], order_expr);
                    node_manager_->RegisterNode(new_order_op);
                    PhysicalJoinNode* new_join_op = new PhysicalJoinNode(
                        new_order_op, join_op->GetProducers()[1],
                        join_op->join_type_, join_op->condition_);
                    node_manager_->RegisterNode(new_order_op);
                    *output = new_join_op;
                    return true;
                }
            }

            break;
        }

        case kPhysicalOpGroupAndSort: {
            auto group_sort_op = dynamic_cast<PhysicalGroupAndSortNode*>(in);
            if (node::ExprListNullOrEmpty(group_sort_op->groups_) &&
                (nullptr == group_sort_op->orders_ ||
                 node::ExprListNullOrEmpty(
                     group_sort_op->orders_->order_by_))) {
                LOG(WARNING) << "LeftJoin group and sort optimized skip: both "
                                "order and groups are empty ";
            }
            if (kPhysicalOpJoin == in->GetProducers()[0]->type_) {
                if (kPhysicalOpJoin == in->GetProducers()[0]->type_) {
                    PhysicalJoinNode* join_op =
                        dynamic_cast<PhysicalJoinNode*>(in->GetProducers()[0]);

                    if (node::kJoinTypeLeft != join_op->join_type_) {
                        // skip optimized for other join type
                        return false;
                    }

                    if (!CheckExprListFromSchema(
                            group_sort_op->groups_,
                            join_op->GetProducers()[0]->output_schema)) {
                        return false;
                    }

                    if (!CheckExprListFromSchema(
                            group_sort_op->orders_->order_by_,
                            join_op->GetProducers()[0]->output_schema)) {
                        return false;
                    }

                    // 符合优化条件
                    PhysicalGroupAndSortNode* new_group_sort_op =
                        new PhysicalGroupAndSortNode(join_op->GetProducers()[0],
                                                     group_sort_op->groups_,
                                                     group_sort_op->orders_);
                    node_manager_->RegisterNode(new_group_sort_op);
                    PhysicalJoinNode* new_join_op = new PhysicalJoinNode(
                        new_group_sort_op, join_op->GetProducers()[1],
                        join_op->join_type_, join_op->condition_);
                    node_manager_->RegisterNode(new_join_op);
                    *output = new_join_op;
                    return true;
                }
            }

            break;
        }
        default: {
            return false;
        }
    }
    return false;
}  // namespace vm
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

// transform project plan in request mode
bool TransformRequestQuery::TransformProjecPlantOp(
    const node::ProjectPlanNode* node, PhysicalOpNode** output,
    base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status.msg;
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

        PhysicalOpNode* project_op;
        if (!TransformProjectOp(project_list, depend, &project_op, status)) {
            return false;
        }
        ops.push_back(project_op);
    }

    if (ops.empty()) {
        status.msg = "fail transform project op: empty projects";
        status.code = common::kPlanError;
        LOG(WARNING) << status.msg;
        return false;
    }

    if (ops.size() == 1) {
        *output = ops[0];
        return true;
    } else {
        auto iter = ops.cbegin();

        PhysicalOpNode* join = new PhysicalJoinNode(
            (*iter), *(++iter), ::fesql::node::kJoinTypeConcat, nullptr);
        node_manager_->RegisterNode(join);
        iter++;
        for (; iter != ops.cend(); iter++) {
            join = new PhysicalJoinNode(
                join, *iter, ::fesql::node::kJoinTypeConcat, nullptr);
            node_manager_->RegisterNode(join);
        }

        auto project_list = node_manager_->MakeProjectListPlanNode(nullptr);
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
                    for (auto column : depend->output_schema) {
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

        return CreatePhysicalProjectNode(kTableProject, join, project_list,
                                         output, status);
    }
}
}  // namespace vm
}  // namespace fesql
