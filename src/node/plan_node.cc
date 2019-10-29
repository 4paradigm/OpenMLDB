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

int PlanNode::GetChildrenSize() {
    return children_.size();
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

bool BinaryPlanNode::AddChild(PlanNode *node) {
    if (children_.size() >= 2) {
        LOG(WARNING) << "cannot add more than 2 children into binary plan node";
        return false;
    }
    children_.push_back(node);
    return true;
}

bool MultiChildPlanNode::AddChild(PlanNode *node) {
    children_.push_back(node);
    return true;
}

std::string NameOfPlanNodeType(const PlanType &type) {
    switch (type) {
        case kSelect:return std::string("kSelect");
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

}
}
