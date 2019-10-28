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

const std::string SPACE_ST = "+- ";
const std::string SPACE_ED = "";
const std::string INDENT = "|\t";

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
std::string NameOfPlanNodeType(const PlanType &type);
class PlanNode {
public:
    PlanNode(PlanType type) : type_(type) {};
    int GetChildrenSize();
    virtual bool AddChild(PlanNode *node) = 0;

    virtual PlanType GetType() const {
        return type_;
    }

    std::vector<PlanNode *> &GetChildren() {
        return children_;
    }

    friend std::ostream &operator<<(std::ostream &output, const PlanNode &thiz);

    virtual void Print(std::ostream &output, const std::string &tab) const {
        output << tab << SPACE_ST << plan::NameOfPlanNodeType(type_);
    }
    virtual void PrintVector(std::ostream &output, const std::string &tab, std::vector<PlanNode *> vec) const {
        if (0 == vec.size()) {
            output << tab << "[]";
            return;
        }
        output << tab << "[\n";
        const std::string space = tab + INDENT;
        for (auto child : vec) {
            child->Print(output, space);
            output << "\n";
        }
        output << tab << "]";
    }
    virtual void PrintChildren(std::ostream &output, const std::string &tab) const {
        PrintVector(output, tab, children_);
    }

protected:
    PlanType type_;
    std::vector<PlanNode *> children_;
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
    virtual void Print(std::ostream &output, const std::string &org_tab) const {
        const std::string tab = org_tab + INDENT + SPACE_ED;
        const std::string space = org_tab + INDENT + INDENT;
        output << "\n";
        PlanNode::Print(output, org_tab);
        output << tab << SPACE_ST << "children:\n";
        PlanNode::PrintChildren(output, tab);
    }
};

class BinaryPlanNode : public PlanNode {
public:
    BinaryPlanNode(PlanType type) : PlanNode(type) {};
    virtual bool AddChild(PlanNode *node);
    virtual void Print(std::ostream &output, const std::string &org_tab) const {
        const std::string tab = org_tab + INDENT + SPACE_ED;
        const std::string space = org_tab + INDENT + INDENT;
        PlanNode::Print(output, org_tab);
        output << "\n";
        output << tab << "children:\n";
        PlanNode::PrintChildren(output, tab);
    }

};

class MultiChildPlanNode : public PlanNode {
public:
    MultiChildPlanNode(PlanType type) : PlanNode(type) {};
    virtual bool AddChild(PlanNode *node);
    virtual void Print(std::ostream &output, const std::string &org_tab) const {
        const std::string tab = org_tab + INDENT + SPACE_ED;
        const std::string space = org_tab + INDENT + INDENT;
        PlanNode::Print(output, org_tab);
        output << "\n";
        output << tab << SPACE_ST << "children:\n";
        PlanNode::PrintChildren(output, tab);
    }
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
    ProjectPlanNode(parser::SQLNode *expression)
        : LeafPlanNode(kProject), expression_(expression), name_(""), w_("") {};
    ProjectPlanNode(parser::SQLNode *expression, const std::string &name)
        : LeafPlanNode(kProject), expression_(expression), name_(name) {};

    ProjectPlanNode(parser::SQLNode *expression,
                    const std::string &name,
                    const std::string &table,
                    const std::string &w)
        : LeafPlanNode(kProject), expression_(expression), name_(name), table_(table), w_(w) {};

    void Print(std::ostream &output, const std::string &orgTab) const {
        PlanNode::Print(output, orgTab);
        const std::string tab = orgTab + INDENT;
        const std::string space = tab + INDENT;
        output << "\n";
        expression_->Print(output, space);
    }

    std::string GetW() const {
        return w_;
    }

    std::string GetTable() const {
        return table_;
    }
    std::string GetName() const {
        return name_;
    }

    parser::SQLNode *GetExpression() const {
        return expression_;
    }

    bool IsWindowProject() {
        return !w_.empty();
    }

private:
    parser::SQLNode *expression_;
    std::string name_;
    std::string w_;
    std::string table_;

};

class ProjectListPlanNode : public MultiChildPlanNode {
public:
    ProjectListPlanNode() : MultiChildPlanNode(kProjectList) {};
    ProjectListPlanNode(const std::string &table, const std::string &w) : MultiChildPlanNode(kProjectList), table_(table), w_(w) {};
    void Print(std::ostream &output, const std::string &org_tab) const {
        PlanNode::Print(output, org_tab);
        const std::string tab = org_tab + INDENT + SPACE_ED;
        const std::string space = org_tab + INDENT + INDENT;
        output << "\n";
        if (w_.empty()) {
            output << tab << SPACE_ST << "table: " << table_ << ", projects:\n";
            PlanNode::PrintVector(output, space, projects);
        } else {
            output << tab << SPACE_ST << "window: " << w_ << ", projects:\n";
            PlanNode::PrintVector(output, space, projects);
        }
    }

    std::vector<PlanNode *> &GetProjects() {
        return projects;
    }
    void AddProject(ProjectPlanNode *project) {
        projects.push_back((PlanNode *) project);
    }

private:
    std::vector<PlanNode *> projects;
    std::string w_;
    std::string table_;
};

}
}

#endif //FESQL_PLAN_PLANNODE_H
