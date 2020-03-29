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

bool PlanListEquals(const std::vector<PlanNode *> &list1,
                    const std::vector<PlanNode *> &list2) {
    if (list1.size() != list2.size()) {
        return false;
    }
    auto iter1 = list1.cbegin();
    auto iter2 = list2.cbegin();
    while (iter1 != list1.cend()) {
        if (!(*iter1)->Equals(*iter2)) {
            return false;
        }
        iter1++;
        iter2++;
    }
    return true;
}

bool PlanEquals(const PlanNode *left, const PlanNode *right) {
    return left == right ? true : nullptr == left ? false : left->Equals(right);
}

void PlanNode::Print(std::ostream &output, const std::string &tab) const {
    output << tab << SPACE_ST << "[" << node::NameOfPlanNodeType(type_) << "]";
}

bool PlanNode::Equals(const PlanNode *that) const {
    if (this == that) {
        return true;
    }
    if (nullptr == that || type_ != that->type_) {
        return false;
    }
    return PlanListEquals(this->children_, that->children_);
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
bool LeafPlanNode::Equals(const PlanNode *that) const {
    return PlanNode::Equals(that);
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
bool UnaryPlanNode::Equals(const PlanNode *that) const {
    return PlanNode::Equals(that);
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
bool BinaryPlanNode::Equals(const PlanNode *that) const {
    return PlanNode::Equals(that);
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
bool MultiChildPlanNode::Equals(const PlanNode *that) const {
    return PlanNode::Equals(that);
}

void ProjectNode::Print(std::ostream &output, const std::string &orgTab) const {
    PlanNode::Print(output, orgTab);
    output << "\n";
    PrintValue(output, orgTab + INDENT, expression_->GetExprString(), name_,
               false);
}
bool ProjectNode::Equals(const PlanNode *node) const {
    if (nullptr == node) {
        return false;
    }

    if (this == node) {
        return true;
    }

    if (type_ != node->type_) {
        return false;
    }
    const ProjectNode *that = dynamic_cast<const ProjectNode *>(node);
    return this->name_ == that->name_ &&
           node::ExprEquals(this->expression_, that->expression_) &&
           LeafPlanNode::Equals(node);
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
        case kPlanTypeRename:
            return std::string("kPlanTypeRename");
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
        output << org_tab << SPACE_ST << item_name << ":"
               << "\n";
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
        output << "\n";
        PrintPlanNode(output, org_tab + INDENT, (w_ptr_), "", false);
        output << "\n";
        PrintPlanVector(output, org_tab + INDENT, projects,
                        "projects on window ", false);
    }
}
bool ProjectListNode::Equals(const PlanNode *node) const {
    if (nullptr == node) {
        return false;
    }

    if (this == node) {
        return true;
    }

    if (type_ != node->type_) {
        return false;
    }
    const ProjectListNode *that = dynamic_cast<const ProjectListNode *>(node);
    if (this->projects.size() != that->projects.size()) {
        return false;
    }

    return this->is_window_agg_ == that->is_window_agg_ &&
           node::PlanEquals(this->w_ptr_, that->w_ptr_) &&
           PlanListEquals(this->projects, that->projects) &&
           LeafPlanNode::Equals(node);
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
bool ProjectPlanNode::Equals(const PlanNode *node) const {
    if (nullptr == node) {
        return false;
    }

    if (this == node) {
        return true;
    }

    if (type_ != node->type_) {
        return false;
    }
    const ProjectPlanNode *that = dynamic_cast<const ProjectPlanNode *>(node);
    return PlanListEquals(this->project_list_vec_, that->project_list_vec_) &&
           UnaryPlanNode::Equals(node);
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
bool LimitPlanNode::Equals(const PlanNode *node) const {
    if (nullptr == node) {
        return false;
    }

    if (this == node) {
        return true;
    }

    if (type_ != node->type_) {
        return false;
    }
    const LimitPlanNode *that = dynamic_cast<const LimitPlanNode *>(node);
    return this->limit_cnt_ == that->limit_cnt_ && UnaryPlanNode::Equals(node);
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
bool FilterPlanNode::Equals(const PlanNode *node) const {
    if (nullptr == node) {
        return false;
    }

    if (this == node) {
        return true;
    }

    if (type_ != node->type_) {
        return false;
    }
    const FilterPlanNode *that = dynamic_cast<const FilterPlanNode *>(node);
    return ExprEquals(this->condition_, that->condition_) &&
           UnaryPlanNode::Equals(node);
}
void TablePlanNode::Print(std::ostream &output,
                          const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";

    PrintValue(output, org_tab + "\t", table_, "table", true);
}
bool TablePlanNode::Equals(const PlanNode *node) const {
    if (nullptr == node) {
        return false;
    }

    if (this == node) {
        return true;
    }

    if (type_ != node->type_) {
        return false;
    }
    const TablePlanNode *that = dynamic_cast<const TablePlanNode *>(node);
    return table_ == that->table_ && LeafPlanNode::Equals(that);
}
void RenamePlanNode::Print(std::ostream &output,
                           const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    PrintValue(output, org_tab + "\t", table_, "table", true);
    output << "\n";
    PrintChildren(output, org_tab);
}
bool RenamePlanNode::Equals(const PlanNode *node) const {
    if (nullptr == node) {
        return false;
    }

    if (this == node) {
        return true;
    }

    if (type_ != node->type_) {
        return false;
    }
    const RenamePlanNode *that = dynamic_cast<const RenamePlanNode *>(node);
    return table_ == that->table_ && UnaryPlanNode::Equals(that);
}
void WindowPlanNode::Print(std::ostream &output,
                           const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    PrintValue(output, org_tab, name, "window_name", true);
}
bool WindowPlanNode::Equals(const PlanNode *node) const {
    if (nullptr == node) {
        return false;
    }

    if (this == node) {
        return true;
    }

    if (type_ != node->type_) {
        return false;
    }
    const WindowPlanNode *that = dynamic_cast<const WindowPlanNode *>(node);
    return this->name == that->name &&
           this->is_range_between_ == that->is_range_between_ &&
           this->start_offset_ == that->start_offset_ &&
           this->end_offset_ == that->end_offset_ &&
           this->start_offset_ == that->end_offset_ &&
           this->orders_ == that->orders_ && this->keys_ == that->keys_ &&
           LeafPlanNode::Equals(node);
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
bool SortPlanNode::Equals(const PlanNode *node) const {
    if (nullptr == node) {
        return false;
    }

    if (this == node) {
        return true;
    }

    if (type_ != node->type_) {
        return false;
    }
    const SortPlanNode *that = dynamic_cast<const SortPlanNode *>(node);
    return node::ExprEquals(this->order_list_, that->order_list_) &&
           UnaryPlanNode::Equals(that);
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
bool GroupPlanNode::Equals(const PlanNode *node) const {
    if (nullptr == node) {
        return false;
    }

    if (this == node) {
        return true;
    }

    if (type_ != node->type_) {
        return false;
    }
    const GroupPlanNode *that = dynamic_cast<const GroupPlanNode *>(node);
    return node::ExprEquals(this->by_list_, that->by_list_) &&
           UnaryPlanNode::Equals(that);
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
bool JoinPlanNode::Equals(const PlanNode *node) const {
    if (nullptr == node) {
        return false;
    }

    if (this == node) {
        return true;
    }

    if (type_ != node->type_) {
        return false;
    }
    const JoinPlanNode *that = dynamic_cast<const JoinPlanNode *>(node);
    return join_type_ == that->join_type_ &&
           node::ExprEquals(this->condition_, that->condition_) &&
           BinaryPlanNode::Equals(that);
}
void UnionPlanNode::Print(std::ostream &output,
                          const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    std::string tab = org_tab + INDENT;
    PrintValue(output, tab, is_all ? "ALL" : "DISTINCT", "union_type", false);
    output << "\n";
    PrintChildren(output, org_tab);
}
bool UnionPlanNode::Equals(const PlanNode *node) const {
    if (nullptr == node) {
        return false;
    }

    if (this == node) {
        return true;
    }

    if (type_ != node->type_) {
        return false;
    }
    const UnionPlanNode *that = dynamic_cast<const UnionPlanNode *>(node);
    return this->is_all == that->is_all && BinaryPlanNode::Equals(that);
}
void QueryPlanNode::Print(std::ostream &output,
                          const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    PrintPlanNode(output, org_tab + INDENT, children_[0], "", true);
}
bool QueryPlanNode::Equals(const PlanNode *node) const {
    return UnaryPlanNode::Equals(node);
}
}  // namespace node
}  // namespace fesql
