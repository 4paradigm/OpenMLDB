//
// Created by 陈靓 on 2019/10/24.
//

#ifndef FESQL_PLAN_PLANNODE_H
#define FESQL_PLAN_PLANNODE_H

#include <glog/logging.h>
#include <list>
#include <vector>
#include "emun.h"
#include <node/sql_node.h>
namespace fesql {
namespace node {


std::string NameOfPlanNodeType(const PlanType &type);

class PlanNode {
public:
    PlanNode(PlanType type) : type_(type) {};
    virtual ~PlanNode() {
//        LOG(INFO) << "plan node " << NameOfPlanNodeType(type_) << " distruction enter";
    }
    int GetChildrenSize();
    virtual bool AddChild(PlanNode *node) {};

    virtual PlanType GetType() const {
        return type_;
    }

    std::vector<PlanNode*> &GetChildren() {
        return children_;
    }

    friend std::ostream &operator<<(std::ostream &output, const PlanNode &thiz);

    virtual void Print(std::ostream &output, const std::string &tab) const {
        output << tab << SPACE_ST << "plan["<<node::NameOfPlanNodeType(type_)<<"]";
    }




protected:
    PlanType type_;
    std::vector<PlanNode*> children_;
};

typedef std::vector<PlanNode*> PlanNodeList;
class LeafPlanNode : public PlanNode {
public:
    LeafPlanNode(PlanType type) : PlanNode(type) {};
    virtual bool AddChild(PlanNode *node);

};
class UnaryPlanNode : public PlanNode {

public:
    UnaryPlanNode(PlanType type) : PlanNode(type) {};
    virtual bool AddChild(PlanNode *node);
    virtual void Print(std::ostream &output, const std::string &org_tab) const;
};

class BinaryPlanNode : public PlanNode {
public:
    BinaryPlanNode(PlanType type) : PlanNode(type) {};
    virtual bool AddChild(PlanNode *node);
    virtual void Print(std::ostream &output, const std::string &org_tab) const;

};

class MultiChildPlanNode : public PlanNode {
public:
    MultiChildPlanNode(PlanType type) : PlanNode(type) {};
    virtual bool AddChild(PlanNode *node);
    virtual void Print(std::ostream &output, const std::string &org_tab) const;
};

class SelectPlanNode : public MultiChildPlanNode {
public:
    SelectPlanNode() : MultiChildPlanNode(kSelect), limit_cnt_(-1) {};
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
    ProjectPlanNode(node::SQLNode *expression)
        : LeafPlanNode(kProject), expression_(expression), name_(""), w_("") {};
    ProjectPlanNode(node::SQLNode *expression, const std::string &name)
        : LeafPlanNode(kProject), expression_(expression), name_(name) {};

    ProjectPlanNode(node::SQLNode *expression,
                    const std::string &name,
                    const std::string &table,
                    const std::string &w)
        : LeafPlanNode(kProject), expression_(expression), name_(name), table_(table), w_(w) {};

    void Print(std::ostream &output, const std::string &orgTab) const;
    std::string GetW() const {
        return w_;
    }

    std::string GetTable() const {
        return table_;
    }
    std::string GetName() const {
        return name_;
    }

    node::SQLNode *GetExpression() const {
        return expression_;
    }

    bool IsWindowProject() {
        return !w_.empty();
    }

private:
    node::SQLNode *expression_;
    std::string name_;
    std::string w_;
    std::string table_;

};

class ProjectListPlanNode : public MultiChildPlanNode {
public:
    ProjectListPlanNode() : MultiChildPlanNode(kProjectList) {};
    ProjectListPlanNode(const std::string &table, const std::string &w) : MultiChildPlanNode(kProjectList), table_(table), w_(w) {};
    void Print(std::ostream &output, const std::string &org_tab) const;

    PlanNodeList &GetProjects() {
        return projects;
    }
    void AddProject(ProjectPlanNode *project) {
        projects.push_back((PlanNode *) project);
    }

private:
    PlanNodeList projects;
    std::string w_;
    std::string table_;
};


void PrintPlanVector(std::ostream &output,
                     const std::string &tab,
                     PlanNodeList vec,
                     const std::string vector_name,
                     bool last_item);





void PrintPlanNode(std::ostream &output,
                   const std::string &org_tab,
                   PlanNode *node_ptr,
                   const std::string &item_name,
                   bool last_child);

}
}

#endif //FESQL_PLAN_PLANNODE_H
