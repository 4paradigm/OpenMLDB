//
// Created by 陈靓 on 2019/10/24.
//

#ifndef FESQL_PLANNER_H
#define FESQL_PLANNER_H

#endif //FESQL_PLANNER_H

#include "parser/node.h"
namespace fesql {
namespace plan {

class Planner {
public:
    Planner() {
    }

    ~Planner() {
    }

    virtual bool CreatePlan() = 0;
};

class SimplePlanner : public Planner {
public:
    SimplePlanner(::fesql::parser::SQLNode *root) : parser_tree_ptr_(root) {

    }
    const ::fesql::parser::SQLNode* GetParserTree() const {
        return parser_tree_ptr_;
    }

    bool CreatePlan();
private:
    ::fesql::parser::SQLNode* parser_tree_ptr_;
};

}
}