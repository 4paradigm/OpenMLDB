/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * planner_test.cc
 *
 * Author: chenjing
 * Date: 2019/10/24
 *--------------------------------------------------------------------------
**/

#include "parser/parser.h"
#include "plan/planner.h"
#include "gtest/gtest.h"
namespace fesql {
namespace plan {

using fesql::node::SQLNode;
using fesql::node::SQLNodeList;
using fesql::node::PlanNode;
using fesql::node::NodeManager;
// TODO: add ut: 检查SQL的语法树节点预期 2019.10.23
class PlannerTest : public ::testing::Test {

public:
    PlannerTest() {
        manager_ = new NodeManager();
        parser_ = new parser::FeSQLParser();

    }

    ~PlannerTest() {}
protected:
    parser::FeSQLParser *parser_;
    NodeManager *manager_;
};

TEST_F(PlannerTest, SimplePlannerCreatePlanTest) {
    node::NodePointVector list;
    int ret = parser_->parse("SELECT t1.COL1 c1,  trim(COL3) as trimCol3, COL2 FROM t1 limit 10;", list, manager_);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1, list.size());

    Planner *planner_ptr = new SimplePlanner(manager_);
    PlanNode *plan_ptr = planner_ptr->CreatePlan(list[0]);
    ASSERT_TRUE(NULL != plan_ptr);
    std::cout << *plan_ptr << std::endl;
    // validate select plan
    ASSERT_EQ(node::kSelect, plan_ptr->GetType());
    node::SelectPlanNode *select_ptr = (node::SelectPlanNode *) plan_ptr;
    // validate limit 10
    ASSERT_EQ(node::kPlanTypeLimit, select_ptr->GetChildren()[0]->GetType());
    node::LimitPlanNode *limit_node = (node::LimitPlanNode *) select_ptr->GetChildren()[0];
    ASSERT_EQ(10,limit_node->GetLimitCnt());

    // validate project list based on current row
    std::vector<PlanNode *> plan_vec = limit_node->GetChildren();
    ASSERT_EQ(1, plan_vec.size());
    ASSERT_EQ(node::kProjectList, plan_vec.at(0)->GetType());
    ASSERT_EQ(3, ((node::ProjectListPlanNode *) plan_vec.at(0))->GetProjects().size());

    delete planner_ptr;

}

TEST_F(PlannerTest, SimplePlannerCreatePlanWithWindowProjectTest) {
    node::NodePointVector list;
    int ret = parser_->parse(
        "SELECT t1.COL1 c1,  trim(COL3) as trimCol3, COL2 , max(t1.age) over w1 FROM t1 limit 10;",
        list, manager_);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1, list.size());

    std::cout << *(list[0]) << std::endl;
    Planner *planner_ptr = new SimplePlanner(manager_);
    PlanNode *plan_ptr = planner_ptr->CreatePlan(list[0]);
    ASSERT_TRUE(NULL != plan_ptr);

    std::cout << *plan_ptr << std::endl;
    // validate select plan
    ASSERT_EQ(node::kSelect, plan_ptr->GetType());
    node::SelectPlanNode *select_ptr = (node::SelectPlanNode *) plan_ptr;
    // validate limit 10
    // validate limit 10
    ASSERT_EQ(node::kPlanTypeLimit, select_ptr->GetChildren()[0]->GetType());
    node::LimitPlanNode *limit_node = (node::LimitPlanNode *) select_ptr->GetChildren()[0];
    ASSERT_EQ(10,limit_node->GetLimitCnt());

    // validate project list based on current row
    std::vector<PlanNode *> plan_vec = limit_node->GetChildren();
    ASSERT_EQ(2, plan_vec.size());
    ASSERT_EQ(node::kProjectList, plan_vec.at(0)->GetType());
    ASSERT_EQ(3, ((node::ProjectListPlanNode *) plan_vec.at(0))->GetProjects().size());
    ASSERT_EQ(node::kProjectList, plan_vec.at(1)->GetType());
    ASSERT_EQ(1, ((node::ProjectListPlanNode *) plan_vec.at(1))->GetProjects().size());
    delete planner_ptr;

}

}
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}