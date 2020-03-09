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
    output << tab << SPACE_ST << "plan[" << node::NameOfPlanNodeType(type_)
           << "]\n";
    PrintPlanVector(output, tab + "\t", children_, "children", true);
}

bool LeafPlanNode::AddChild(PlanNode *node) {
    LOG(WARNING) << "cannot add child into leaf plan node";
    return false;
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
    PrintPlanVector(output, org_tab, children_, "children", true);
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
    PrintPlanVector(output, org_tab, children_, "children", true);
}

bool MultiChildPlanNode::AddChild(PlanNode *node) {
    children_.push_back(node);
    return true;
}

void MultiChildPlanNode::Print(std::ostream &output,
                               const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    PrintPlanVector(output, org_tab + INDENT, children_, "children", true);
}

void ProjectNode::Print(std::ostream &output, const std::string &orgTab) const {
    PlanNode::Print(output, orgTab);
    output << "\n";
    PrintSQLNode(output, orgTab, expression_, "expression", false);
    output << "\n";
    PrintValue(output, orgTab, std::to_string(pos_), "org_pos", false);
    output << "\n";
    PrintValue(output, orgTab, name_, "name", true);
}

std::string NameOfPlanNodeType(const PlanType &type) {
    switch (type) {
        case kPlanTypeSelect:
            return std::string("kSelectPlan");
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
            return "kFilter";
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
        case kPlanTypeDistinct:
            return "kDistinctPlan";
        case kProjectList:
            return std::string("kProjectList");
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
    output << tab << SPACE_ST << vector_name << "[list]: \n";
    const std::string space = last_item ? (tab + INDENT) : tab + OR_INDENT;
    int i = 0;
    int vec_size = vec.size();
    for (i = 0; i < vec_size - 1; ++i) {
        PrintPlanNode(output, space, vec[i], "" + std::to_string(i), false);
        output << "\n";
    }
    PrintPlanNode(output, space, vec[i], "" + std::to_string(i), true);
}

void PrintPlanNode(std::ostream &output, const std::string &org_tab,
                   const PlanNode *node_ptr, const std::string &item_name,
                   bool last_child) {
    output << org_tab << SPACE_ST << item_name << ":";

    if (nullptr == node_ptr) {
        output << " null";
    } else if (last_child) {
        output << "\n";
        node_ptr->Print(output, org_tab + INDENT);
    } else {
        output << "\n";
        node_ptr->Print(output, org_tab + OR_INDENT);
    }
}

void ProjectListNode::Print(std::ostream &output,
                            const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    if (nullptr == w_ptr_) {
        PrintPlanVector(output, org_tab + INDENT, projects,
                        "projects on table ", false);
    } else {
        PrintPlanNode(output, org_tab, (w_ptr_), "window", false);
        output << "\n";
        PrintPlanVector(output, org_tab + INDENT, projects,
                        "projects on window ", false);
        output << "\n";
    }
    output << "\n";
    PrintPlanVector(output, org_tab + INDENT, children_, "children", true);
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
    PrintPlanVector(output, org_tab + "\t", project_list_vec_, "project_plan",
                    true);
}
void LimitPlanNode::Print(std::ostream &output,
                          const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    PrintValue(output, org_tab, std::to_string(limit_cnt_), "limit_cnt", true);
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
    PrintValue(output, org_tab, name, "window name", true);
}
}  // namespace node
}  // namespace fesql
