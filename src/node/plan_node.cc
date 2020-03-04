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
           << "]";
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

void ProjectNode::Print(std::ostream &output,
                            const std::string &orgTab) const {
    PlanNode::Print(output, orgTab);
    output << "\n";
    PrintSQLNode(output, orgTab, expression_, "expression", false);
    output << "\n";
    PrintValue(output, orgTab, std::to_string(pos_), "org_pos", true);
    output << "\n";
    PrintValue(output, orgTab, name_, "name", true);
}

void ProjectListPlanNode::Print(std::ostream &output,
                                const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    if (nullptr == w_ptr_) {
        PrintPlanVector(output, org_tab + INDENT, projects,
                        "projects on table ", true);
    } else {
        PrintSQLNode(
            output, org_tab,
            const_cast<SQLNode *>(dynamic_cast<const SQLNode *>(w_ptr_)),
            "window", false);
        PrintPlanVector(output, org_tab + INDENT, projects,
                        "projects on window ", true);
        output << "\n";
    }
    output << "\n";
    PrintPlanVector(output, org_tab + INDENT, children_, "children", true);
}

std::string NameOfPlanNodeType(const PlanType &type) {
    switch (type) {
        case kPlanTypeSelect:
            return std::string("kSelect");
        case kPlanTypeScan:
            return std::string("kScan");
        case kPlanTypeMerge:
            return std::string("kMerge");
        case kPlanTypeLimit:
            return std::string("kLimit");
        case kProjectList:
            return std::string("kProjectList");
        case kProject:
            return std::string("kProject");
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
                   PlanNode *node_ptr, const std::string &item_name,
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

void FuncDefPlanNode::Print(std::ostream &output,
                            const std::string &orgTab) const {
    PlanNode::Print(output, orgTab);
    output << "\n";
    PrintSQLNode(output, orgTab, fn_def_, "fun_def", true);
}
}  // namespace node
}  // namespace fesql
