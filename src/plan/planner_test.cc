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

// TODO: add ut: 检查SQL的语法树节点预期 2019.10.23
class PlannerTest : public ::testing::Test {

public:
    PlannerTest() {
        parser_ = new parser::FeSQLParser();
        manager_ = new node::NodeManager();
    }

    ~PlannerTest() {}
protected:
    parser::FeSQLParser *parser_;
    node::NodeManager *manager_;
};

TEST_F(PlannerTest, SimplePlannerTest) {
    fesql::node::SQLNode *root = manager_->MakeSQLNode(node::kSelectStmt);
    SimplePlanner planner_ptr(root);
    ASSERT_EQ(root, planner_ptr.GetParserTree());
}

TEST_F(PlannerTest, SimplePlannerCreatePlanTest) {
    node::SQLNodeList *list = manager_->MakeNodeList();
    int ret = parser_->parse("SELECT t1.COL1 c1,  trim(COL3) as trimCol3, COL2 FROM t1 limit 10;", list, manager_);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1, list->Size());
    node::SQLLinkedNode *ptr = list->GetHead();

    Planner *planner_ptr = new SimplePlanner(ptr->node_ptr_);
    node::PlanNode *plan_ptr = planner_ptr->CreatePlan();
    ASSERT_TRUE(NULL != plan_ptr);
    std::cout << *plan_ptr << std::endl;
    // validate select plan
    ASSERT_EQ(node::kSelect, plan_ptr->GetType());
    node::SelectPlanNode *select_ptr = (node::SelectPlanNode *) plan_ptr;
    // validate limit 10
    ASSERT_EQ(10, select_ptr->GetLimitCount());

    // validate project list based on current row
    std::vector<node::PlanNode *> plan_vec = select_ptr->GetChildren();
    ASSERT_EQ(1, plan_vec.size());
    ASSERT_EQ(node::kProjectList, plan_vec.at(0)->GetType());
    ASSERT_EQ(3, ((node::ProjectListPlanNode *) plan_vec.at(0))->GetProjects().size());

    delete planner_ptr;

}

TEST_F(PlannerTest, SimplePlannerCreatePlanWithWindowProjectTest) {
    node::SQLNodeList *list = manager_->MakeNodeList();
    int ret = parser_->parse(
        "SELECT t1.COL1 c1,  trim(COL3) as trimCol3, COL2 , max(t1.age) over w1 FROM t1 limit 10;",
        list, manager_);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1, list->Size());
    node::SQLLinkedNode *ptr = list->GetHead();

    Planner *planner_ptr = new SimplePlanner(ptr->node_ptr_);
    node::PlanNode *plan_ptr = planner_ptr->CreatePlan();
    ASSERT_TRUE(NULL != plan_ptr);

    std::cout << *plan_ptr << std::endl;
    // validate select plan
    ASSERT_EQ(node::kSelect, plan_ptr->GetType());
    node::SelectPlanNode *select_ptr = (node::SelectPlanNode *) plan_ptr;
    // validate limit 10
    ASSERT_EQ(10, select_ptr->GetLimitCount());

    // validate project list based on current row
    std::vector<node::PlanNode *> plan_vec = select_ptr->GetChildren();
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