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

#include "node/plan_node.h"
#include <string>
namespace hybridse {
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
    PrintValue(output, orgTab + INDENT, expression_->GetExprString(),
               "[" + std::to_string(pos_) + "]" + name_, false);

    if (nullptr != frame_) {
        output << "\n";
        PrintValue(output, orgTab + INDENT, frame_->GetExprString(), "frame",
                   true);
    }
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
           node::SqlEquals(this->frame_, that->frame_) &&
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

bool ProjectListNode::MergeProjectList(node::ProjectListNode *project_list1,
                                       node::ProjectListNode *project_list2,
                                       node::ProjectListNode *merged_project) {
    if (nullptr == project_list1 || nullptr == project_list2 ||
        nullptr == merged_project) {
        LOG(WARNING) << "can't merge project list: input projects or output "
                        "projects is null";
        return false;
    }
    auto iter1 = project_list1->GetProjects().cbegin();
    auto end1 = project_list1->GetProjects().cend();
    auto iter2 = project_list2->GetProjects().cbegin();
    auto end2 = project_list2->GetProjects().cend();
    while (iter1 != end1 && iter2 != end2) {
        auto project1 = dynamic_cast<node::ProjectNode *>(*iter1);
        auto project2 = dynamic_cast<node::ProjectNode *>(*iter2);
        if (project1->GetPos() < project2->GetPos()) {
            merged_project->AddProject(project1);
            iter1++;
        } else {
            merged_project->AddProject(project2);
            iter2++;
        }
    }
    while (iter1 != end1) {
        auto project1 = dynamic_cast<node::ProjectNode *>(*iter1);
        merged_project->AddProject(project1);
        iter1++;
    }
    while (iter2 != end2) {
        auto project2 = dynamic_cast<node::ProjectNode *>(*iter2);
        merged_project->AddProject(project2);
        iter2++;
    }
    return true;
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
bool ProjectListNode::IsSimpleProjectList() {
    if (is_window_agg_) {
        return false;
    }
    if (projects.empty()) {
        return false;
    }
    for (auto item : projects) {
        auto expr = dynamic_cast<ProjectNode *>(item)->GetExpression();
        if (!node::ExprIsSimple(expr)) {
            return false;
        }
    }
    return true;
}
void FuncDefPlanNode::Print(std::ostream &output,
                            const std::string &orgTab) const {
    PlanNode::Print(output, orgTab);
    output << "\n";
    PrintSqlNode(output, orgTab + "\t", fn_def_, "fun_def", true);
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
bool ProjectPlanNode::IsSimpleProjectPlan() {
    if (project_list_vec_.empty()) {
        return false;
    }

    for (auto item : project_list_vec_) {
        auto project_list = dynamic_cast<node::ProjectListNode *>(item);
        if (!project_list->IsSimpleProjectList()) {
            return false;
        }
    }
    return true;
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

    PrintValue(output, org_tab + "\t", table_,
               is_primary_ ? "primary_table" : "table", true);
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
           SqlEquals(this->frame_node_, that->frame_node_) &&
           this->orders_ == that->orders_ && this->keys_ == that->keys_ &&
           PlanListEquals(this->union_tables_, that->union_tables_) &&
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
    if (nullptr != orders_) {
        output << "\n";
        PrintValue(output, tab, ExprString(orders_), "orders", true);
    }
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
           node::ExprEquals(this->orders_, that->orders_) &&
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

void CreatePlanNode::Print(std::ostream &output, const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    std::string tab = org_tab + INDENT;
    if (!database_.empty()) {
        PrintValue(output, tab, database_, "database", false);
        output << "\n";
    }
    PrintValue(output, tab, table_name_, "table", false);
    output << "\n";
    PrintSqlVector(output, tab, column_desc_list_, "column_desc_list", false);
    output << "\n";
    PrintValue(output, tab, std::to_string(replica_num_), "replica_num", false);
    output << "\n";
    PrintValue(output, tab, std::to_string(partition_num_), "partition_num", false);
    output << "\n";
    PrintSqlVector(output, tab, distribution_list_, "distribution", false);
}
}  // namespace node
}  // namespace hybridse
