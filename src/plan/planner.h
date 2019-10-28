//
// Created by 陈靓 on 2019/10/24.
//

#ifndef FESQL_PLANNER_H
#define FESQL_PLANNER_H

#endif //FESQL_PLANNER_H

#include "parser/node.h"
#include "plan_node.h"
#include "glog/logging.h"
namespace fesql {
namespace plan {

class Planner {
public:
    Planner() {
    }

    ~Planner() {
    }

    virtual PlanNode *CreatePlan() = 0;
protected:
    PlanNode *CreatePlanRecurse(parser::SQLNode *root);
    PlanNode *CreateSelectPlan(parser::SelectStmt *root);
    PlanNode *CreateProjectPlanNode(parser::SQLNode *root, std::string table_name);
    PlanNode *CreateDataProviderPlanNode(parser::SQLNode *root);
    PlanNode *CreateDataCollectorPlanNode(parser::SQLNode *root);
};

class SimplePlanner : public Planner {
public:
    SimplePlanner(::fesql::parser::SQLNode *root) : parser_tree_ptr_(root) {

    }
    const ::fesql::parser::SQLNode *GetParserTree() const {
        return parser_tree_ptr_;
    }

    PlanNode *CreatePlan();
private:

    ::fesql::parser::SQLNode *parser_tree_ptr_;
};

}
}