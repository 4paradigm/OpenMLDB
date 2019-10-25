//
// Created by 陈靓 on 2019/10/24.
//

#ifndef FESQL_PLAN_PLANNODE_H
#define FESQL_PLAN_PLANNODE_H

#include <glog/logging.h>
#include <list>
#include <vector>
#include <parser/node.h>
namespace fesql {
namespace plan {

/**
 * Planner:
 *  basic class for plan
 *
 */
enum PlanType {
    kSelect,
    kProjectList,
    kProject,
    kExpr,
    kOpExpr,
    kScalarFunction,
    kAggFunction,
    kAggWindowFunction,
    kUnknow,
};
std::ostream &operator<<(std::ostream &output, const fesql::plan::PlanType &thiz);
class PlanNode {
public:
    PlanNode(PlanType type) : type_(type) {};
    int GetChildrenSize();
    virtual bool AddChild(PlanNode *node) = 0;

protected:
    PlanType type_;
    std::list<PlanNode *> children_;
};

class LeafPlanNode : public PlanNode {
public:
    LeafPlanNode(PlanType type) : PlanNode(type) {};
    virtual bool AddChild(PlanNode *node);

};
class UnaryPlanNode : public PlanNode {

public:
    UnaryPlanNode(PlanType type) : PlanNode(type) {};
    virtual bool AddChild(PlanNode *node);
};

class BinaryPlanNode : public PlanNode {
public:
    BinaryPlanNode(PlanType type) : PlanNode(type) {};
    virtual bool AddChild(PlanNode *node);

};

class MultiChildPlanNode : public PlanNode {
public:
    MultiChildPlanNode(PlanType type) : PlanNode(type) {};
    virtual bool AddChild(PlanNode *node);
};

class SelectPlanNode : public MultiChildPlanNode {
public:
    SelectPlanNode() : MultiChildPlanNode(kSelect) {};
    virtual bool AddChild(PlanNode *node);
    int GetLimitCount() {
        return limit_cnt_;
    }

    bool SetLimitCount(int count) {
        limit_cnt_ = count;
    }

private:
    int limit_cnt_;
};

class ProjectPlanNode : public LeafPlanNode {
public:
    ProjectPlanNode() : LeafPlanNode(kProject) {};
    ProjectPlanNode(parser::SQLNode *expression) : LeafPlanNode(kProject), expression_(expression), name_("") {};
    ProjectPlanNode(parser::SQLNode *expression, const std::string &name) : LeafPlanNode(kProject), expression_(expression), name_(name) {};
    virtual bool AddChild(PlanNode *node);
private:
    parser::SQLNode *expression_;
    std::string name_;
};


////// static function or friend function
std::string NameOfPlanNodeType(PlanType &type);
}

}

#endif //FESQL_PLAN_PLANNODE_H
