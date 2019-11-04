/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * plan_node.cc
 *      definitions for query plan nodes
 * Author: chenjing
 * Date: 2019/10/24
 *--------------------------------------------------------------------------
**/

#include "plan_node.h"

namespace fesql {
namespace node {


void PlanNode::Print(std::ostream &output, const std::string &tab) const {
    output << tab << SPACE_ST << "plan[" << node::NameOfPlanNodeType(type_) << "]";
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

void UnaryPlanNode::Print(std::ostream &output, const std::string &org_tab) const {
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

void BinaryPlanNode::Print(std::ostream &output, const std::string &org_tab) const {
    output << "\n";
    PlanNode::Print(output, org_tab);
    PrintPlanVector(output, org_tab, children_, "children", true);
}

bool MultiChildPlanNode::AddChild(PlanNode *node) {
    children_.push_back(node);
    return true;
}

void MultiChildPlanNode::Print(std::ostream &output, const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    PrintPlanVector(output, org_tab + INDENT, children_, "children", true);
}

void ProjectPlanNode::Print(std::ostream &output, const std::string &orgTab) const {
    PlanNode::Print(output, orgTab);
    output << "\n";
    PrintSQLNode(output, orgTab, expression_, "expression", true);

}

void ProjectListPlanNode::Print(std::ostream &output, const std::string &org_tab) const {
    PlanNode::Print(output, org_tab);
    output << "\n";
    if (w_.empty()) {
        PrintPlanVector(output, org_tab + INDENT, projects, "projects on table " + table_, true);
    } else {
        PrintPlanVector(output, org_tab + INDENT, projects, "projects on window " + w_, true);
    }
    output << "\n";
    PrintPlanVector(output, org_tab + INDENT, children_, "children", true);
}

std::string NameOfPlanNodeType(const PlanType &type) {
    switch (type) {
        case kSelect:return std::string("kSelect");
        case kPlanTypeScan:return std::string("kScan");
        case kPlanTypeLimit:return std::string("kLimit");
        case kProjectList:return std::string("kProjectList");
        case kProject:return std::string("kProject");
        case kScalarFunction:return std::string("kScalarFunction");
        case kAggFunction:return std::string("kAggFunction");
        case kAggWindowFunction:return std::string("kAggWindowFunction");
        case kOpExpr:return std::string("kOpExpr");
        case kUnknowPlan: return std::string("kUnknow");
        default: return std::string("unknow");
    }
}

std::ostream &operator<<(std::ostream &output, const PlanNode &thiz) {
    thiz.Print(output, "");
    return output;
}


void PrintPlanVector(std::ostream &output,
                     const std::string &tab,
                     PlanNodeList vec,
                     const std::string vector_name,
                     bool last_item) {
    if (0 == vec.size()) {
        output << tab << SPACE_ST << vector_name << ": []";
        return;
    }
    output << tab << SPACE_ST << vector_name << "[list]: \n";
    const std::string space = last_item ? (tab + INDENT) : tab + OR_INDENT;
    int i = 0;
    for (i = 0; i < vec.size() - 1; ++i) {
        PrintPlanNode(output, space, vec[i], "" + std::to_string(i), false);
        output << "\n";
    }
    PrintPlanNode(output, space, vec[i], "" + std::to_string(i), true);
}

void PrintPlanNode(std::ostream &output,
                   const std::string &org_tab,
                   PlanNode *node_ptr,
                   const std::string &item_name,
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

}
}
