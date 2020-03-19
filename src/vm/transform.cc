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
            ok = TransformProjectOp(
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

bool Transform::TransformProjectOp(const node::ProjectPlanNode* node,
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

    std::vector<PhysicalOpNode*> ops;
    for (auto iter = node->project_list_vec_.cbegin();
         iter != node->project_list_vec_.cend(); iter++) {
        fesql::node::ProjectListNode* project_list =
            dynamic_cast<fesql::node::ProjectListNode*>(*iter);
        if (project_list->is_window_agg_) {
            PhysicalOpNode* project_op;
            if (!TransformWindowProject(project_list, depend, &project_op,
                                        status)) {
                return false;
            }
            ops.push_back(project_op);
        } else {
            PhysicalOpNode* op = nullptr;
            if (kPhysicalOpGroupBy == depend->type_) {
                if (!CreatePhysicalAggrerationNode(
                    depend, project_list->GetProjects(), &op, status)) {
                    return false;
                }
            } else {
                if (!CreatePhysicalRowNode(depend, project_list->GetProjects(), &op, status)) {
                    return false;
                }
            }
            ops.push_back(op);
        }
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
        node::PlanNodeList project_list;
        uint32_t pos = 0;
        for (auto iter = node->pos_mapping_.cbegin();
             iter != node->pos_mapping_.cend(); iter++) {
            auto sub_project_list = dynamic_cast<node::ProjectListNode*>(
                node->project_list_vec_[iter->first]);

            auto project_node = dynamic_cast<node::ProjectNode*>(
                sub_project_list->GetProjects().at(iter->second));
            project_list.push_back(node_manager_->MakeProjectNode(
                pos, project_node->GetName(),
                node_manager_->MakeColumnRefNode(project_node->GetName(), "")));
            pos++;
        }
        return CreatePhysicalRowNode(join, project_list, output, status);
    }
}
bool Transform::TransformWindowProject(const node::ProjectListNode* node,
                                       PhysicalOpNode* depend_node,
                                       PhysicalOpNode** output,
                                       base::Status& status) {
    if (nullptr == node || nullptr == node->w_ptr_ || nullptr == output) {
        status.msg = "project node or window node or output node is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status.msg;
        return false;
    }

    PhysicalOpNode* depend = const_cast<PhysicalOpNode*>(depend_node);
    if (!node->w_ptr_->GetKeys().empty()) {
        // TODO(chenjing): remove 临时适配, 有mem泄漏问题
        node::ExprListNode* expr = node_manager_->MakeExprList();
        for (auto id : node->w_ptr_->GetKeys()) {
            expr->AddChild(node_manager_->MakeColumnRefNode(id, ""));
        }
        PhysicalGroupNode* group_op = new PhysicalGroupNode(depend, expr);
        node_manager_->RegisterNode(group_op);
        depend = group_op;
    }

    if (!node->w_ptr_->GetOrders().empty()) {
        // TODO(chenjing): remove 临时适配, 有mem泄漏问题
        node::ExprListNode* expr = new node::ExprListNode();
        for (auto id : node->w_ptr_->GetOrders()) {
            expr->AddChild(new node::ColumnRefNode(id, ""));
        }
        node::OrderByNode* order_node = dynamic_cast<node::OrderByNode*>(
            node_manager_->MakeOrderByNode(expr, true));
        PhysicalSortNode* sort_op = new PhysicalSortNode(depend, order_node);
        node_manager_->RegisterNode(sort_op);
        depend = sort_op;
    }

    if (node->w_ptr_->GetStartOffset() != -1 ||
        node->w_ptr_->GetEndOffset() != -1) {
        PhysicalBufferNode* window_op =
            new PhysicalBufferNode(depend, node->w_ptr_->GetStartOffset(),
                                   node->w_ptr_->GetEndOffset());
        node_manager_->RegisterNode(window_op);
        depend = window_op;
    }

    Schema output_schema;
    std::string fn_name;
    if (!GenProjects(depend->output_schema, node->GetProjects(), false, fn_name,
                     output_schema, status)) {
        return false;
    }
    *output = new PhysicalAggrerationNode(depend, fn_name, output_schema);
    node_manager_->RegisterNode(*output);
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
        *output = new PhysicalScanTableNode(table);
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
bool Transform::TransformPhysicalPlan(const ::fesql::node::PlanNode* node,
                                      ::fesql::vm::PhysicalOpNode** output,
                                      ::fesql::base::Status& status) {
    PhysicalOpNode* physical_plan;
    if (!TransformPlanOp(node, &physical_plan, status)) {
        return false;
    }

    for (auto type : passes) {
        switch (type) {
            case kPassGroupByOptimized: {
                GroupByOptimized pass(node_manager_, db_, catalog_);
                PhysicalOpNode* new_op;
                if (pass.Apply(physical_plan, &new_op)) {
                    physical_plan = new_op;
                }
                break;
            }
            case kPassSortByOptimized: {
                SortByOptimized pass(node_manager_, db_, catalog_);
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
    AddPass(kPassGroupByOptimized);
    AddPass(kPassSortByOptimized);
    return false;
}
bool Transform::CreatePhysicalAggrerationNode(
    PhysicalOpNode* node, const node::PlanNodeList& projects,
    PhysicalOpNode** output, base::Status& status) {  // NOLINT
    Schema output_schema;
    std::string fn_name;
    if (!GenProjects(node->output_schema, projects, false, fn_name,
                     output_schema, status)) {
        return false;
    }
    PhysicalOpNode* op =
        new PhysicalAggrerationNode(node, fn_name, output_schema);
    node_manager_->RegisterNode(op);
    *output = op;
    return true;
}
bool Transform::CreatePhysicalRowNode(PhysicalOpNode* node,
                                      const node::PlanNodeList& projects,
                                      PhysicalOpNode** output,
                                      base::Status& status) {
    Schema output_schema;
    std::string fn_name;
    if (!GenProjects(node->output_schema, projects, true, fn_name,
                     output_schema, status)) {
        return false;
    }
    PhysicalOpNode* op =
        new PhysicalRowProjectNode(node, fn_name, output_schema);
    node_manager_->RegisterNode(op);
    *output = op;
    return true;
}

bool GroupByOptimized::Transform(PhysicalOpNode* in, PhysicalOpNode** output) {
    switch (in->type_) {
        case kPhysicalOpGroupBy: {
            PhysicalGroupNode* group_op = dynamic_cast<PhysicalGroupNode*>(in);
            if (kPhysicalOpScan == in->GetProducers()[0]->type_) {
                auto scan_op =
                    dynamic_cast<PhysicalScanNode*>(in->GetProducers()[0]);
                if (kScanTypeTableScan == scan_op->scan_type_) {
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
                    // remove node if groups is empty
                    if (new_groups->children_.empty()) {
                        *output = scan_index_op;
                        return true;
                    } else {
                        PhysicalGroupNode* new_group_op =
                            new PhysicalGroupNode(scan_index_op, new_groups);
                        node_manager_->RegisterNode(new_group_op);
                        *output = new_group_op;
                        return true;
                    }
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
bool GroupByOptimized::TransformGroupExpr(const node::ExprListNode* groups,
                                          const IndexHint& index_hint,
                                          std::string* index_name,
                                          const node::ExprListNode** output) {
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
bool GroupByOptimized::MatchBestIndex(const std::vector<std::string>& columns,
                                      const IndexHint& index_hint,
                                      std::vector<bool>* bitmap_ptr,
                                      std::string* index_name) {
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
bool SortByOptimized::Transform(PhysicalOpNode* in, PhysicalOpNode** output) {
    switch (in->type_) {
        case kPhysicalOpSortBy: {
            PhysicalSortNode* sort_op = dynamic_cast<PhysicalSortNode*>(in);
            if (kPhysicalOpScan == in->GetProducers()[0]->type_) {
                auto scan_op =
                    dynamic_cast<PhysicalScanNode*>(in->GetProducers()[0]);
                if (kScanTypeIndexScan == scan_op->scan_type_) {
                    std::string index_name;
                    auto scan_index_op =
                        dynamic_cast<PhysicalScanIndexNode*>(scan_op);

                    const node::OrderByNode* new_order;
                    if (!TransformOrderExpr(sort_op->order_, scan_index_op,
                                            &new_order)) {
                        return false;
                    }

                    // remove node if groups is empty
                    if (nullptr == new_order->order_by_ ||
                        new_order->order_by_->children_.empty()) {
                        *output = scan_index_op;
                        return true;
                    } else {
                        PhysicalSortNode* new_sort_op =
                            new PhysicalSortNode(scan_index_op, new_order);
                        node_manager_->RegisterNode(new_sort_op);
                        *output = new_sort_op;
                        return true;
                    }
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
bool SortByOptimized::TransformOrderExpr(
    const node::OrderByNode* order, const PhysicalScanIndexNode* scan_index_op,
    const node::OrderByNode** output) {
    if (nullptr == order || nullptr == scan_index_op || nullptr == output) {
        LOG(WARNING) << "fail to transform order expr : order expr or scan op "
                        "or output is null";
        return false;
    }
    auto& index_st = scan_index_op->table_handler_->GetIndex().at(
        scan_index_op->index_name_);
    auto& ts_column =
        scan_index_op->table_handler_->GetSchema().Get(index_st.ts_pos);

    std::vector<std::string> columns;
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
    switch (in->type_) {
        case kPhysicalOpGroupBy: {
            auto group_op = dynamic_cast<PhysicalGroupNode*>(in);
            if (nullptr == group_op || nullptr == group_op->groups_ ||
                group_op->groups_->IsEmpty()) {
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
                std::vector<std::string> columns;
                for (auto expr : group_op->groups_->children_) {
                    switch (expr->expr_type_) {
                        case node::kExprColumnRef: {
                            auto column =
                                dynamic_cast<node::ColumnRefNode*>(expr);
                            if (!ColumnExist(join_op->GetProducers()
                                                 .at(0)
                                                 ->output_schema,
                                             column->GetColumnName())) {
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

                auto group_expr = group_op->groups_;
                // 符合优化条件
                PhysicalGroupNode* new_group_op = new PhysicalGroupNode(
                    join_op->GetProducers()[0], group_expr);
                PhysicalJoinNode* new_join_op = new PhysicalJoinNode(
                    new_group_op, join_op->GetProducers()[1],
                    join_op->join_type_, join_op->condition_);
                *output = new_join_op;
                return true;
            }

            break;
        }
        case kPhysicalOpSortBy: {
            auto sort_op = dynamic_cast<PhysicalSortNode*>(in);
            if (nullptr == sort_op || nullptr == sort_op->order_ ||
                nullptr == sort_op->order_->order_by_ ||
                sort_op->order_->order_by_->IsEmpty()) {
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
                    std::vector<std::string> columns;
                    for (auto expr : sort_op->order_->order_by_->children_) {
                        switch (expr->expr_type_) {
                            case node::kExprColumnRef: {
                                auto column =
                                    dynamic_cast<node::ColumnRefNode*>(expr);
                                if (!ColumnExist(join_op->GetProducers()
                                                     .at(0)
                                                     ->output_schema,
                                                 column->GetColumnName())) {
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

                    auto order_expr = sort_op->order_;
                    // 符合优化条件
                    PhysicalSortNode* new_order_op = new PhysicalSortNode(
                        join_op->GetProducers()[0], order_expr);
                    PhysicalJoinNode* new_join_op = new PhysicalJoinNode(
                        new_order_op, join_op->GetProducers()[1],
                        join_op->join_type_, join_op->condition_);
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
}  // namespace vm
}  // namespace fesql
