/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * plan_node.cc
 *      definitions for query plan nodes
 * Author: chenjing
 * Date: 2019/10/24
 *--------------------------------------------------------------------------
 **/

#include "node/plan_node.h"
#include <string>
namespace fesql {
namespace node {

void PlanNode::Print(std::ostream &output, const std::string &tab) const {
    output << tab << SPACE_ST << "[" << node::NameOfPlanNodeType(type_) << "]";
}
void PlanNode::PrintChildren(std::ostream &output,
                             const std::string &tab) const {
    output << "";
}

bool LeafPlanNode::AddChild(PlanNode *node) {
    LOG(WARNING) << "cannot add child into leaf plan node";
    return false;
}
void LeafPlanNode::PrintChildren(std::ostream &output,
                                 const std::string &tab) const {
    output << "";
}

bool UnaryPlanNode::AddChild(PlanNode *node) {
    if (children_.size() >= 1) {
        LOG(WARNING) << "cannot add more than 1 children into unary plan node";
        return false;
    }
    children_.push_back(node);
    return true;
}

void UnaryPlanNode::Print(std::ostream &output,
                          const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    PrintChildren(output, org_tab);
}
void UnaryPlanNode::PrintChildren(std::ostream &output,
                                  const std::string &tab) const {
    PrintPlanNode(output, tab, children_[0], "", true);
}

bool BinaryPlanNode::AddChild(PlanNode *node) {
    if (children_.size() >= 2) {
        LOG(WARNING) << "cannot add more than 2 children into binary plan node";
        return false;
    }
    children_.push_back(node);
    return true;
}

void BinaryPlanNode::Print(std::ostream &output,
                           const std::string &org_tab) const {
    output << "\n";
    PlanNode::Print(output, org_tab);
    output << "\n";
    PrintChildren(output, org_tab);
}

void BinaryPlanNode::PrintChildren(std::ostream &output,
                                   const std::string &tab) const {
    PrintPlanNode(output, tab + INDENT, children_[0], "", true);
    output << "\n";
    PrintPlanNode(output, tab + INDENT, children_[1], "", true);
}

bool MultiChildPlanNode::AddChild(PlanNode *node) {
    children_.push_back(node);
    return true;
}

void MultiChildPlanNode::Print(std::ostream &output,
                               const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    PrintChildren(output, org_tab);
}

void MultiChildPlanNode::PrintChildren(std::ostream &output,
                                       const std::string &tab) const {
    PrintPlanVector(output, tab + INDENT, children_, "children", true);
}

void ProjectNode::Print(std::ostream &output, const std::string &orgTab) const {
    PlanNode::Print(output, orgTab);
    output << "\n";
    PrintValue(output, orgTab + INDENT, expression_->GetExprString(), name_,
               false);
}

std::string NameOfPlanNodeType(const PlanType &type) {
    switch (type) {
        case kPlanTypeQuery:
            return std::string("kQueryPlan");
        case kPlanTypeCmd:
            return "kCmdPlan";
        case kPlanTypeCreate:
            return "kCreatePlan";
        case kPlanTypeInsert:
            return "kInsertPlan";
        case kPlanTypeScan:
            return std::string("kScanPlan");
        case kPlanTypeLimit:
            return std::string("kLimitPlan");
        case kPlanTypeFilter:
            return "kFilterPlan";
        case kPlanTypeProject:
            return std::string("kProjectPlan");
        case kPlanTypeTable:
            return std::string("kTablePlan");
        case kPlanTypeJoin:
            return "kJoinPlan";
        case kPlanTypeUnion:
            return "kUnionPlan";
        case kPlanTypeSort:
            return "kSortPlan";
        case kPlanTypeGroup:
            return "kGroupPlan";
        case kPlanTypeDistinct:
            return "kDistinctPlan";
        case kProjectList:
            return std::string("kProjectList");
        case kPlanTypeWindow:
            return std::string("kWindow");
        case kProjectNode:
            return std::string("kProjectNode");
        case kScalarFunction:
            return std::string("kScalarFunction");
        case kAggFunction:
            return std::string("kAggFunction");
        case kAggWindowFunction:
            return std::string("kAggWindowFunction");
        case kOpExpr:
            return std::string("kOpExpr");
        case kPlanTypeFuncDef:
            return "kPlanTypeFuncDef";
        case kUnknowPlan:
            return std::string("kUnknow");
        default:
            return std::string("unknow");
    }
}

std::ostream &operator<<(std::ostream &output, const PlanNode &thiz) {
    thiz.Print(output, "");
    return output;
}

void PrintPlanVector(std::ostream &output, const std::string &tab,
                     PlanNodeList vec, const std::string vector_name,
                     bool last_item) {
    if (0 == vec.size()) {
        output << tab << SPACE_ST << vector_name << ": []";
        return;
    }
    output << tab << SPACE_ST << vector_name << "[list]: ";
    const std::string space = last_item ? (tab + INDENT) : tab + OR_INDENT;
    int i = 0;
    int vec_size = vec.size();
    for (i = 0; i < vec_size - 1; ++i) {
        output << "\n";
        PrintPlanNode(output, space, vec[i], "", false);
    }
    output << "\n";
    PrintPlanNode(output, space, vec[i], "", true);
}

void PrintPlanNode(std::ostream &output, const std::string &org_tab,
                   const PlanNode *node_ptr, const std::string &item_name,
                   bool last_child) {
    if (!item_name.empty()) {
        output << org_tab << SPACE_ST << item_name << ":" << "\n";
    }

    if (nullptr == node_ptr) {
        output << " null";
    } else if (last_child) {
        node_ptr->Print(output, org_tab);
    } else {
        node_ptr->Print(output, org_tab);
    }
}

void ProjectListNode::Print(std::ostream &output,
                            const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    if (nullptr == w_ptr_) {
        output << "\n";
        PrintPlanVector(output, org_tab + INDENT, projects,
                        "projects on table ", false);
    } else {
        PrintPlanNode(output, org_tab + INDENT, (w_ptr_), "", false);
        output << "\n";
        PrintPlanVector(output, org_tab + INDENT, projects,
                        "projects on window ", false);
    }
}
void FuncDefPlanNode::Print(std::ostream &output,
                            const std::string &orgTab) const {
    PlanNode::Print(output, orgTab);
    output << "\n";
    PrintSQLNode(output, orgTab + "\t", fn_def_, "fun_def", true);
}
void ProjectPlanNode::Print(std::ostream &output,
                            const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    PrintValue(output, org_tab + "\t", table_, "table", false);
    output << "\n";
    PrintPlanVector(output, org_tab + "\t", project_list_vec_,
                    "project_list_vec", true);
    output << "\n";
    PrintChildren(output, org_tab);
}
void LimitPlanNode::Print(std::ostream &output,
                          const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    std::string tab = org_tab + INDENT;
    PrintValue(output, tab, std::to_string(limit_cnt_), "limit_cnt", true);
    output << "\n";
    PrintChildren(output, org_tab);
}

void FilterPlanNode::Print(std::ostream &output,
                           const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    std::string tab = org_tab + INDENT;
    PrintValue(output, tab,
               nullptr == condition_ ? "" : condition_->GetExprString(),
               "condition", true);
    output << "\n";
    PrintChildren(output, org_tab);
}
void TablePlanNode::Print(std::ostream &output,
                          const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";

    PrintValue(output, org_tab + "\t", table_, "table", true);
}
void WindowPlanNode::Print(std::ostream &output,
                           const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    PrintValue(output, org_tab, name, "window_name", true);
}

void SortPlanNode::Print(std::ostream &output,
                         const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    std::string tab = org_tab + INDENT;
    PrintValue(output, tab,
               nullptr == order_list_ ? "()" : order_list_->GetExprString(),
               "order_by", true);
    output << "\n";
    PrintChildren(output, org_tab);
}
void GroupPlanNode::Print(std::ostream &output,
                          const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    std::string tab = org_tab + INDENT;
    PrintValue(output, tab,
               nullptr == by_list_ ? "()" : by_list_->GetExprString(),
               "group_by", true);
    output << "\n";
    PrintChildren(output, org_tab);
}
void JoinPlanNode::Print(std::ostream &output,
                         const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    std::string tab = org_tab + INDENT;
    PrintValue(output, tab, JoinTypeName(join_type_), "type", true);
    output << "\n";
    PrintValue(output, tab,
               nullptr == condition_ ? "" : condition_->GetExprString(),
               "condition", true);
    output << "\n";
    PrintChildren(output, org_tab);
}
void UnionPlanNode::Print(std::ostream &output,
                          const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    std::string tab = org_tab + INDENT;
    PrintValue(output, tab, is_all ? "ALL" : "DISTINCT",
        "union_type", false);
    output << "\n";
    PrintChildren(output, org_tab);
}
void QueryPlanNode::Print(std::ostream &output,
                          const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    PrintPlanNode(output, org_tab + INDENT, children_[0], "", true);
}
}  // namespace node
}  // namespace fesql
