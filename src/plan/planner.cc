//
// Created by 陈靓 on 2019/10/24.
//

#include "planner.h"

namespace fesql {
namespace plan {

int PlanNode::GetChildrenSize() {
    return children.size();
}

bool LeafPlanNode::AddChild(PlanNode *node) {
    LOG(WARNING) << "cannot add child into leaf plan node";
    return false;
}

bool UnaryPlanNode::AddChild(PlanNode *node) {
    if (children.size() >= 1) {
        LOG(WARNING) << "cannot add more than 1 children into unary plan node";
        return false;
    }
    children.push_back(node);
    return true;
}

bool BinaryPlanNode::AddChild(PlanNode *node) {
    if (children.size() >= 2) {
        LOG(WARNING) << "cannot add more than 2 children into binary plan node";
        return false;
    }
    children.push_back(node);
    return true;
}
}
}
