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
    PlannerTest() {}

    ~PlannerTest() {}
};

TEST_F(PlannerTest, SimplePlannerTest) {
    fesql::parser::SQLNode *root = new ::fesql::parser::SQLNode(::fesql::parser::kSelectStmt, 0, 0);
    SimplePlanner *planner_ptr = new SimplePlanner(root);
    ASSERT_EQ(root, planner_ptr->GetParserTree());
}

TEST_F(PlannerTest, SimplePlannerCreatePlanTest) {
    parser::SQLNodeList *list = new parser::SQLNodeList();
    int ret = parser::FeSqlParse("SELECT t1.COL1 c1, COL2, trim(COL3) as trimCol3 FROM t1 limit 10;", list);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1, list->Size());
    parser::SQLLinkedNode * ptr = list->GetHead();

    Planner *planner_ptr = new SimplePlanner(ptr->node_ptr_);
    PlanNode* plan_ptr = planner_ptr->CreatePlan();
    ASSERT_TRUE(NULL != plan_ptr);
    ASSERT_EQ(kSelect, plan_ptr->GetType());
    SelectPlanNode * select_ptr = (SelectPlanNode*)plan_ptr;
    ASSERT_EQ(10, select_ptr->GetLimitCount());
    std::list<PlanNode *> plan_list = select_ptr->GetChildren();
    ASSERT_EQ(1, plan_list.size());

}

}
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}