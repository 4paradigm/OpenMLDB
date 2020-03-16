/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * transform.cc
 *
 * Author: chenjing
 * Date: 2020/3/13
 *--------------------------------------------------------------------------
 **/
#include "vm/transform.h"
#include <stack>
#include <unordered_map>
#include "vm/physical_op.h"

namespace fesql {
namespace vm {

std::ostream& operator<<(std::ostream& output,
                         const fesql::vm::LogicalOp& thiz) {
    return output << *(thiz.node_);
}
bool TransformLogicalTreeToLogicalGraph(const ::fesql::node::PlanNode* node,
                                        fesql::base::Status& status,  // NOLINT
                                        LogicalGraph& graph) {        // NOLINT
    if (nullptr == node) {
        status.msg = "node is null";
        status.code = common::kOpGenError;
        return false;
    }
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

Transform::Transform(const std::shared_ptr<Catalog>& catalog)
    : catalog_(catalog) {}

Transform::~Transform() {}

bool Transform::TransformPhysicalPlan(const ::fesql::node::PlanNode* node,
                                      ::fesql::vm::PhysicalOpNode** ouput,
                                      ::fesql::base::Status& status) {
    if (nullptr == node || nullptr == ouput) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        return false;
    }
    LogicalOp logical_op = LogicalOp(node);
    auto map_iter = op_map.find(logical_op);
    // logical plan node already exist
    if (map_iter != op_map.cend()) {
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
            ok = TransformScanOp(dynamic_cast<const ::fesql::node::TablePlanNode*>(node), &op,
                                 status);
            break;
        case node::kPlanTypeQuery:
            ok = TransformPhysicalPlan(dynamic_cast<const ::fesql::node::QueryPlanNode*>(node), &op,
                                 status);
            break;
        case node::kPlanTypeRename:
            ok = TransformRenameOp(dynamic_cast<const ::fesql::node::RenamePlanNode*>(node), &op, status));

    }
    if (!ok) {
        return false;
    }
    op_map[logical_op] = op;
    *ouput = op;
    return true;
}

bool Transform::TransformLimitOp(const node::LimitPlanNode* node,
                                 PhysicalOpNode** output,
                                 base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        return false;
    }
    PhysicalOpNode* depend = nullptr;
    if (!TransformPhysicalPlan(node->GetChildren()[0], &depend, status)) {
        return false;
    }
    *output = new PhysicalLimitNode(depend, node->limit_cnt_);
    return true;
}

bool Transform::TransformProjectOp(const node::ProjectPlanNode* node,
                                   PhysicalOpNode** output,
                                   base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        return false;
    }
    PhysicalOpNode* depend = nullptr;
    if (!TransformPhysicalPlan(node->GetChildren()[0], &depend, status)) {
        return false;
    }

    std::vector<PhysicalOpNode*> ops;
    for (auto iter = node->project_list_vec_.cbegin();
         iter != node->project_list_vec_.cend(); iter++) {
        fesql::node::ProjectListNode* project_list =
            dynamic_cast<fesql::node::ProjectListNode*>(*iter);
        PhysicalOpNode* op;
        if (project_list->is_window_agg_) {
            PhysicalOpNode* project_op;
            if (!TransformWindowProject(project_list, depend, &project_op,
                                        status)) {
                return false;
            }
            ops.push_back(project_op);
        } else {
            ops.push_back(new PhysicalRowProjectNode(
                depend, &(project_list->GetProjects())));
        }
    }

    if (ops.empty()) {
        status.msg = "fail transform project op: empty projects";
        status.code = common::kPlanError;
        return false;
    }

    if (ops.size() == 1) {
        *output = ops[0];
        return true;
    } else {
        auto iter = ops.cbegin();

        PhysicalOpNode* join = new PhysicalJoinNode(
            (*iter), *(++iter), ::fesql::node::kJoinTypeAppend, nullptr);
        iter++;
        for (; iter != ops.cend(); iter++) {
            join = new PhysicalJoinNode(
                join, *iter, ::fesql::node::kJoinTypeAppend, nullptr);
        }
        *output = join;
        return true;
    }
}
bool Transform::TransformWindowProject(const node::ProjectListNode* node,
                                       PhysicalOpNode* depend_node,
                                       PhysicalOpNode** output,
                                       base::Status& status) {
    if (nullptr == node || nullptr == node->w_ptr_ || nullptr == output) {
        status.msg = "project node or window node or output node is null";
        status.code = common::kPlanError;
        return false;
    }

    PhysicalOpNode* depend = const_cast<PhysicalOpNode*>(depend_node);
    if (!node->w_ptr_->GetKeys().empty()) {
        PhysicalGroupNode* group_op =
            new PhysicalGroupNode(depend, node->w_ptr_->GetKeys());
        depend = group_op;
    }

    if (!node->w_ptr_->GetOrders().empty()) {
        PhysicalSortNode* sort_op =
            new PhysicalSortNode(depend, node->w_ptr_->GetOrders());
        depend = sort_op;
    }

    if (node->w_ptr_->GetStartOffset() != -1 ||
        node->w_ptr_->GetEndOffset() != -1) {
        PhysicalWindowNode* window_op =
            new PhysicalWindowNode(depend, node->w_ptr_->GetStartOffset(),
                                   node->w_ptr_->GetEndOffset());
        depend = window_op;
    }
    *output = new PhysicalAggrerationNode(depend, &(node->GetProjects()));
    return true;
}
bool Transform::TransformJoinOp(const node::JoinPlanNode* node,
                                PhysicalOpNode** output, base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        return false;
    }
    PhysicalOpNode* left = nullptr;
    PhysicalOpNode* right = nullptr;
    if (!TransformPhysicalPlan(node->GetChildren()[0], &left, status)) {
        return false;
    }
    if (!TransformPhysicalPlan(node->GetChildren()[1], &right, status)) {
        return false;
    }
    *output =
        new PhysicalJoinNode(left, right, node->join_type_, node->condition_);
    return true;
}
bool Transform::TransformUnionOp(const node::UnionPlanNode* node,
                                 PhysicalOpNode** output,
                                 base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        return false;
    }
    PhysicalOpNode* left = nullptr;
    PhysicalOpNode* right = nullptr;
    if (!TransformPhysicalPlan(node->GetChildren()[0], &left, status)) {
        return false;
    }
    if (!TransformPhysicalPlan(node->GetChildren()[1], &right, status)) {
        return false;
    }
    *output = new PhysicalUnionNode(left, right, node->is_all);
    return true;
}
bool Transform::TransformGroupOp(const node::GroupPlanNode* node,
                                 PhysicalOpNode** output,
                                 base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        return false;
    }
    PhysicalOpNode* left = nullptr;
    if (!TransformPhysicalPlan(node->GetChildren()[0], &left, status)) {
        return false;
    }
    *output = new PhysicalGroupNode(left, node->by_list_);
    return true;
}
bool Transform::TransformSortOp(const node::SortPlanNode* node,
                                PhysicalOpNode** output, base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        return false;
    }
    PhysicalOpNode* left = nullptr;
    if (!TransformPhysicalPlan(node->GetChildren()[0], &left, status)) {
        return false;
    }
    *output = new PhysicalSortNode(left, node->order_list_);
    return true;
}
bool Transform::TransformFilterOp(const node::FilterPlanNode* node,
                                  PhysicalOpNode** output,
                                  base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        return false;
    }
    PhysicalOpNode* depend = nullptr;
    if (!TransformPhysicalPlan(node->GetChildren()[0], &depend, status)) {
        return false;
    }
    *output = new PhysicalFliterNode(depend, node->condition_);
    return true;
}

bool Transform::TransformScanOp(const node::TablePlanNode* node,
                                PhysicalOpNode** output, base::Status& status) {
    if (nullptr == node || nullptr == output) {
        status.msg = "input node or output node is null";
        status.code = common::kPlanError;
        return false;
    }
    *output = new PhysicalScanTableNode(catalog_->GetTable(node->db_, node->table_));
    return true;
}

// return optimized filter condition and scan index name
bool Transform::TryOptimizedFilterCondition(const IndexHint& index_map,
                                            const node::ExprNode* condition,
                                            std::string& index_name,  // NOLINT
                                            node::ExprNode** output) {
    return false;
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
    if (!TransformPhysicalPlan(node->GetChildren()[0], &left, status)) {
        return false;
    }
    *output = new PhysicaRenameNode(left, node->table_);
    return true;
}

}  // namespace vm
}  // namespace fesql
