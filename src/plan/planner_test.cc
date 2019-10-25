/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * planner_test.cc
 *      
 * Author: chenjing
 * Date: 2019/10/24 
 *--------------------------------------------------------------------------
**/

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
    fesql::parser::SQLNode *root = new ::fesql::parser::SQLNode(::fesql::parser::kSelectStmt, 0, 0);
    Planner *planner_ptr = new SimplePlanner(root);
    ASSERT_EQ(true, planner_ptr->CreatePlan());
}



}
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}