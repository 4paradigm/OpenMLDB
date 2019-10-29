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

class Planner {
public:
    Planner() {
        node_manager_ = new node::NodeManager();
    }

    virtual ~Planner() {
        delete node_manager_;
    }

    virtual node::PlanNode *CreatePlan() = 0;
protected:
    node::PlanNode *CreatePlanRecurse(node::SQLNode *root);
    node::PlanNode *CreateSelectPlan(node::SelectStmt *root);
    node::ProjectPlanNode*CreateProjectPlanNode(node::SQLNode *root, std::string table_name);
    node::PlanNode *CreateDataProviderPlanNode(node::SQLNode *root);
    node::PlanNode *CreateDataCollectorPlanNode(node::SQLNode *root);
    node::NodeManager *node_manager_;
};

class SimplePlanner : public Planner {
public:
    SimplePlanner(::fesql::node::SQLNode *root) : parser_tree_ptr_(root) {

    }
    const ::fesql::node::SQLNode *GetParserTree() const {
        return parser_tree_ptr_;
    }

    node::PlanNode *CreatePlan();
private:

    ::fesql::node::SQLNode *parser_tree_ptr_;
};

}
}