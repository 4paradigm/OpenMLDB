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
namespace plan {

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

std::string NameOfPlanNodeType(PlanType &type) {
    switch (type) {
        case kProjectList:return std::string("kProjectList");
        case kProject:return std::string("kProject");
        case kExpr:return std::string("kExpr");
        case kScalarFunction:return std::string("kScalarFunction");
        case kAggFunction:return std::string("kAggFunction");
        case kAggWindowFunction:return std::string("kAggWindowFunction");
        case kOpExpr:return std::string("kOpExpr");
        case kUnknow: return std::string("kUnknow");
        default: return std::string("unknow");
    }
}


//bool SelectPlanNode::AddChild(PlanNode *node) {
//    return MultiChildPlanNode::AddChild(node);
//}
//bool ProjectPlanNode::AddChild(PlanNode *node) {
//    return LeafPlanNode::AddChild(node);
//}
}
}
