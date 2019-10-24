//
// Created by 陈靓 on 2019/10/24.
//

#ifndef FESQL_PLAN_PLANNODE_H
#define FESQL_PLAN_PLANNODE_H

#include <glog/logging.h>
#include <list>
namespace fesql {
namespace plan {

/**
 * Planner:
 *  basic class for plan
 *
 */
class PlanNode {

public:
    int GetChildrenSize();

    virtual bool AddChild(PlanNode *node) = 0;

protected:
    std::list<PlanNode *> children;
};

class LeafPlanNode : public PlanNode {
public:
    bool AddChild(PlanNode *node);

};
class UnaryPlanNode : public PlanNode {

public:
    bool AddChild(PlanNode *node);
};

class BinaryPlanNode : public PlanNode {
public:
    bool AddChild(PlanNode *node);

};

}
}

#endif //FESQL_PLAN_PLANNODE_H
