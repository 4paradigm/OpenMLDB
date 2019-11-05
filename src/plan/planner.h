//
// Created by 陈靓 on 2019/10/24.
//

#ifndef FESQL_PLANNER_H
#define FESQL_PLANNER_H

#endif //FESQL_PLANNER_H

#include "node/node_manager.h"
#include "node/sql_node.h"
#include "node/plan_node.h"
#include "glog/logging.h"
namespace fesql {
namespace plan {

using node::NodePointVector;
using node::PlanNodeList;
using node::SQLNode;
using node::PlanNode;

class Planner {
public:
    Planner(node::NodeManager *manager) : node_manager_(manager) {
    }

    virtual ~Planner() {
    }

    virtual int CreatePlanTree(NodePointVector &parser_trees, PlanNodeList &plan_trees) = 0;
protected:
    int CreatePlanRecurse(node::SQLNode *root, PlanNode* plan_tree);
    int CreateSelectPlan(node::SQLNode *root, PlanNode* plan_tree);
    int CreateProjectPlanNode(node::SQLNode *root, std::string table_name, node::ProjectPlanNode * plan_tree);
    int CreateDataProviderPlanNode(node::SQLNode *root, PlanNode *plan_tree);
    int CreateDataCollectorPlanNode(node::SQLNode *root, PlanNode *plan_tree);
    node::NodeManager *node_manager_;
};

class SimplePlanner : public Planner {
public:
    SimplePlanner(node::NodeManager *manager) : Planner(manager) {
    }
    int CreatePlanTree(NodePointVector &parser_trees, PlanNodeList &plan_trees);
private:

};

}
}