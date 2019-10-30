//
// Created by 陈靓 on 2019/10/24.
//

#ifndef FESQL_PLANNER_H
#define FESQL_PLANNER_H

#endif //FESQL_PLANNER_H

#include "node/node_memory.h"
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

    virtual PlanNode *CreatePlan(SQLNode *parser_tree_ptr) = 0;
protected:
    node::PlanNode *CreatePlanRecurse(node::SQLNode *root);
    node::PlanNode *CreateSelectPlan(node::SelectStmt *root);
    node::ProjectPlanNode *CreateProjectPlanNode(node::SQLNode *root, std::string table_name);
    node::PlanNode *CreateDataProviderPlanNode(node::SQLNode *root);
    node::PlanNode *CreateDataCollectorPlanNode(node::SQLNode *root);
    node::NodeManager *node_manager_;
};

class SimplePlanner : public Planner {
public:
    SimplePlanner(node::NodeManager *manager) : Planner(manager) {
    }
    PlanNode *CreatePlan(SQLNode *parser_tree_ptr);
private:

};

}
}